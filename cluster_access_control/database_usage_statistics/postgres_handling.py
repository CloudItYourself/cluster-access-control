from datetime import datetime
from typing import Final, Optional

import psycopg2
import psycopg2.extras
from psycopg2 import sql

from cluster_access_control.utilities.environment import ClusterAccessConfiguration


class PostgresHandler:
    DB_PORT: Final[int] = 5432
    NODE_DETAILS_NAME: Final[str] = "nodes_usage"
    NODE_USAGE_TABLE: Final[str] = "nodes_usage_details"
    DB_NAME: Final[str] = "node_metrics"
    SECONDS_IN_DAY: Final[int] = 86400

    def __init__(self):
        self._postgres_details = ClusterAccessConfiguration().get_postgres_details()
        self._connection: Optional[psycopg2.extensions.connection] = None
        self._cursor: Optional[psycopg2.extensions.cursor] = None
        self._create_database_if_doesnt_exist()
        self._initialize_databases()

    def _create_database_if_doesnt_exist(self):
        try:
            # Attempt to connect to the desired database
            self._connection = psycopg2.connect(dbname=PostgresHandler.DB_NAME, user=self._postgres_details.user,
                                                password=self._postgres_details.password,
                                                host=self._postgres_details.host,
                                                port=PostgresHandler.DB_PORT)
            self._connection.autocommit = True
            self._cursor = self._connection.cursor()
        except psycopg2.Error as e:
            # If the connection fails, check if the database does not exist
            if 'database "{}" does not exist'.format(PostgresHandler.DB_NAME) in str(e):
                # Connect to the default database
                connection = psycopg2.connect(dbname='postgres', user=self._postgres_details.user,
                                              password=self._postgres_details.password,
                                              host=self._postgres_details.host,
                                              port=PostgresHandler.DB_PORT)
                connection.autocommit = True
                cursor = connection.cursor()

                # Create the new database
                cursor.execute(f"CREATE DATABASE {PostgresHandler.DB_NAME};")

                # Close the cursor and connection to the default database
                cursor.close()
                connection.close()

                # Now, reconnect to the newly created database
                self._connection = psycopg2.connect(dbname=PostgresHandler.DB_NAME, user=self._postgres_details.user,
                                                    password=self._postgres_details.password,
                                                    host=self._postgres_details.host,
                                                    port=PostgresHandler.DB_PORT)
                self._connection.autocommit = True
                self._cursor = self._connection.cursor()
            else:
                raise RuntimeError(
                    "Error: Could not connect to PostgreSQL. Please check your credentials and database settings.")

    def _initialize_databases(self):
        # SQL command to create a table if it doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {PostgresHandler.NODE_DETAILS_NAME} (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) UNIQUE,
            registration_time timestamp,
            abrupt_disconnects INTEGER
        );
        DO $$ BEGIN
            CREATE TYPE DAY_OF_WEEK AS ENUM ('Sunday', 'Monday', 'Tuesday',
                        'Wednesday', 'Thursday', 'Friday', 'Saturday');
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;

        CREATE TABLE IF NOT EXISTS {PostgresHandler.NODE_USAGE_TABLE} (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            day_of_week DAY_OF_WEEK,
            second_since_midnight INTEGER,
            check_in_count INTEGER
        );
        """
        # Execute the SQL command
        self._cursor.execute(create_table_query)

    def _node_already_registered(self, nodes_name: str) -> bool:
        self._cursor.execute(
            f"SELECT EXISTS(SELECT 1 FROM {PostgresHandler.NODE_DETAILS_NAME} WHERE name='{nodes_name}')")
        return self._cursor.fetchone()[0]

    def register_node(self, node_name: str) -> bool:
        if self._node_already_registered(node_name):
            return True
        table_init = []
        for day_in_week in ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']:
            for i in range(0, PostgresHandler.SECONDS_IN_DAY):
                table_init.append((node_name, day_in_week, i, '0'))

        # Start a transaction
        self._cursor.execute("BEGIN;")
        try:
            # Register node query
            register_node_query = sql.SQL("""
            DO $$ BEGIN
               INSERT INTO {}
               VALUES (DEFAULT, %s, to_timestamp(%s), 0);
            END $$;
            """).format(sql.Identifier(PostgresHandler.NODE_DETAILS_NAME))
            self._cursor.execute(register_node_query, (node_name, datetime.utcnow().timestamp()))

            insert_query = f"insert into {PostgresHandler.NODE_USAGE_TABLE} values %s"
            psycopg2.extras.execute_values(self._cursor, insert_query, table_init, template="(DEFAULT, %s, %s,%s,%s)",
                                           page_size=100)

            # Commit the transaction
            self._cursor.execute("COMMIT;")
        except Exception as e:
            # Rollback the transaction in case of any error
            self._cursor.execute("ROLLBACK;")
            print("Error occurred:", e)


if __name__ == '__main__':
    xd = PostgresHandler()
    xd.register_node("Ronen2")
