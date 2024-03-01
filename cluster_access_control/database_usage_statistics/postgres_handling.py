from datetime import datetime
from typing import Final, Optional

import psycopg2

from cluster_access_control.utilities.environment import ClusterAccessConfiguration


class PostgresHandler:
    DB_PORT: Final[int] = 5432
    NODE_DETAILS_NAME: Final[str] = "nodes_usage"
    NODE_USAGE_TABLE: Final[str] = "nodes_usage_details"
    DB_NAME: Final[str] = "node_metrics"

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
            name VARCHAR(100) UNIQUE,
            day_of_week DAY_OF_WEEK,
            second_since_midnight INTEGER,
            check_in_count INTEGER
        );
        """
        # Execute the SQL command
        self._cursor.execute(create_table_query)

    def register_node(self, node_name: str):
        register_node_query = f"""
        DO $$ BEGIN
           IF NOT EXISTS (SELECT * FROM {PostgresHandler.NODE_DETAILS_NAME} 
                           WHERE name='{node_name}') THEN
               INSERT INTO {PostgresHandler.NODE_DETAILS_NAME}
               VALUES (DEFAULT, '{node_name}', to_timestamp({datetime.utcnow().timestamp()}), 0);
           END IF;
        END $$;
        """
        self._cursor.execute(register_node_query)


if __name__ == '__main__':
    xd = PostgresHandler()
    xd.register_node("Ronen")