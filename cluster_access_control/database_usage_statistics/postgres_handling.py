import time
from datetime import datetime, timedelta
from typing import Final, List

import psycopg2
import psycopg2.extras
from psycopg2 import sql
from psycopg2 import pool

from cluster_access_control.utilities.environment import ClusterAccessConfiguration


class PostgresHandler:
    DB_PORT: Final[int] = 5432
    NODE_DETAILS_NAME: Final[str] = "nodes_usage"
    NODE_USAGE_TABLE: Final[str] = "nodes_usage_details"
    DB_NAME: Final[str] = "node_metrics"
    SECONDS_IN_DAY: Final[int] = 86400
    SECONDS_PER_CHECK_IN: Final[int] = 10

    def __init__(self):
        self._postgres_details = ClusterAccessConfiguration().get_postgres_details()
        self._create_database_if_doesnt_exist()

        self._connection_pool = psycopg2.pool.ThreadedConnectionPool(
            5,  # Minimum number of connections
            20,  # Maximum number of connections
            user=self._postgres_details.user,
            password=self._postgres_details.password,
            host=self._postgres_details.host,
            port=PostgresHandler.DB_PORT,
            database=PostgresHandler.DB_NAME,
        )

        self._initialize_databases()

    def _create_database_if_doesnt_exist(self):
        try:
            # Attempt to connect to the desired database
            connection = psycopg2.connect(
                dbname=PostgresHandler.DB_NAME,
                user=self._postgres_details.user,
                password=self._postgres_details.password,
                host=self._postgres_details.host,
                port=PostgresHandler.DB_PORT,
            )
        except psycopg2.Error as e:
            # If the connection fails, check if the database does not exist
            if 'database "{}" does not exist'.format(PostgresHandler.DB_NAME) in str(e):
                # Connect to the default database
                connection = psycopg2.connect(
                    dbname="postgres",
                    user=self._postgres_details.user,
                    password=self._postgres_details.password,
                    host=self._postgres_details.host,
                    port=PostgresHandler.DB_PORT,
                )
                connection.autocommit = True
                cursor = connection.cursor()

                # Create the new database
                cursor.execute(f"CREATE DATABASE {PostgresHandler.DB_NAME};")

                # Close the cursor and connection to the default database
                cursor.close()
                connection.close()
            else:
                raise RuntimeError(
                    "Error: Could not connect to PostgreSQL. Please check your credentials and database settings."
                )

    def _initialize_databases(self):
        # SQL command to create a table if it doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {PostgresHandler.NODE_DETAILS_NAME} (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) UNIQUE,
            registration_time timestamp,
            abrupt_disconnects INTEGER
        );
        """
        try:
            conn = self._connection_pool.getconn()
            conn.autocommit = True
            with conn.cursor() as cur:
                # Execute the SQL command
                cur.execute(create_table_query)
        finally:
            self._connection_pool.putconn(conn)

    def node_registered(self, nodes_name: str) -> bool:
        try:
            conn = self._connection_pool.getconn()
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT EXISTS(SELECT 1 FROM {PostgresHandler.NODE_DETAILS_NAME} WHERE name='{nodes_name}')"
                )
                return cur.fetchone()[0]
        finally:
            self._connection_pool.putconn(conn)

    def register_node(self, node_name: str) -> bool:
        if self.node_registered(node_name):
            return True
        table_init = []
        for i in range(7):
            for j in range(
                0,
                PostgresHandler.SECONDS_IN_DAY // PostgresHandler.SECONDS_PER_CHECK_IN,
            ):
                table_init.append((i, j, "0"))
        try:
            conn = self._connection_pool.getconn()
            with conn.cursor() as cur:
                cur.execute("BEGIN;")
                try:
                    # Register node query
                    register_node_query = sql.SQL(
                        f"""
                    DO $$ BEGIN
                        CREATE TABLE IF NOT EXISTS {PostgresHandler.NODE_USAGE_TABLE}_{node_name} (
                            id SERIAL PRIMARY KEY,
                            day_of_week smallint,
                            seconds_since_midnight_divided smallint,
                            check_in_count INTEGER
                        );
                       INSERT INTO {PostgresHandler.NODE_DETAILS_NAME}
                       VALUES (DEFAULT, %s, to_timestamp(%s), 0);
                       CREATE INDEX idx_seconds_since_midnight_divided_{node_name} ON {PostgresHandler.NODE_USAGE_TABLE}_{node_name} (seconds_since_midnight_divided);
                    END $$;
                    """
                    )
                    cur.execute(
                        register_node_query, (node_name, datetime.utcnow().timestamp())
                    )

                    insert_query = f"insert into {PostgresHandler.NODE_USAGE_TABLE}_{node_name} values %s"
                    psycopg2.extras.execute_values(
                        cur,
                        insert_query,
                        table_init,
                        template="(DEFAULT, %s, %s, %s)",
                        page_size=100,
                    )

                    # Commit the transaction
                    cur.execute("COMMIT;")
                    return True
                except Exception as e:
                    # Rollback the transaction in case of any error
                    cur.execute("ROLLBACK;")
                    print("Error occurred:", e)
                    return False
        finally:
            self._connection_pool.putconn(conn)

    @staticmethod
    def get_seconds_since_midnight(timestamp: datetime) -> int:
        return (
            timestamp - timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        ).seconds // PostgresHandler.SECONDS_PER_CHECK_IN

    def update_node(self, node_name: str, timestamp: datetime) -> bool:
        if not self.node_registered(node_name):
            return False

        try:
            conn = self._connection_pool.getconn()
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {PostgresHandler.NODE_USAGE_TABLE}_{node_name} SET check_in_count=check_in_count + 1"
                    f" WHERE seconds_since_midnight_divided={PostgresHandler.get_seconds_since_midnight(timestamp)}"
                    f" and day_of_week={timestamp.weekday()}"
                )
        finally:
            self._connection_pool.putconn(conn)

    @staticmethod
    def get_work_days_between_dates_in_order(start_day: int, end_day: int) -> List[int]:
        days = [start_day]

        if start_day == end_day:
            return days

        current_day = start_day
        while current_day != end_day:
            current_day = (current_day + 1) % 7
            days.append(current_day)
        return days

    def get_node_check_in_times(
        self,
        node_name: str,
        start_day: int,
        end_day: int,
        start_time: datetime.timestamp,
        end_time: datetime.timestamp,
    ) -> dict[int, list]:
        if not self.node_registered(node_name):
            raise RuntimeWarning("Node is not registered")
        sql_queries = dict()
        work_days_in_timeframe = PostgresHandler.get_work_days_between_dates_in_order(
            start_day, end_day
        )
        for i, work_day in enumerate(work_days_in_timeframe):
            sql_queries[work_day] = (
                f"SELECT seconds_since_midnight_divided, check_in_count"
                f" FROM {PostgresHandler.NODE_USAGE_TABLE}_{node_name} "
                f"WHERE day_of_week={work_day} "
                f"and seconds_since_midnight_divided>={PostgresHandler.get_seconds_since_midnight(start_time)} "
                f"and seconds_since_midnight_divided<={PostgresHandler.get_seconds_since_midnight(end_time)}"
            )

        sql_results = dict()
        try:
            conn = self._connection_pool.getconn()
            conn.autocommit = True
            with conn.cursor() as cur:
                for work_day, query in sql_queries.items():
                    cur.execute(query)
                    sql_results[work_day] = cur.fetchall()
            return sql_results
        finally:
            self._connection_pool.putconn(conn)

    def get_node_registration_time(self, node_name: str) -> datetime:
        if not self.node_registered(node_name):
            raise RuntimeWarning("Node is not registered")
        try:
            conn = self._connection_pool.getconn()
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT registration_time FROM {PostgresHandler.NODE_DETAILS_NAME} WHERE name='{node_name}'"
                )
                return cur.fetchall()[0][0]
        finally:
            self._connection_pool.putconn(conn)

    def add_abrupt_disconnect_to_node(self, node_name: str) -> bool:
        if not self.node_registered(node_name):
            return False
        try:
            conn = self._connection_pool.getconn()
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {PostgresHandler.NODE_DETAILS_NAME} SET abrupt_disconnects=abrupt_disconnects+1 WHERE name='{node_name}'"
                )
            return True
        finally:
            self._connection_pool.putconn(conn)

    def get_abrupt_disconnect_for_node(self, node_name: str) -> int:
        if not self.node_registered(node_name):
            return -1
        try:
            conn = self._connection_pool.getconn()
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT abrupt_disconnects FROM {PostgresHandler.NODE_DETAILS_NAME} WHERE name='{node_name}'"
                )
                return cur.fetchall()[0][0]
        finally:
            self._connection_pool.putconn(conn)