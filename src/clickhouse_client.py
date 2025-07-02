"""
Module for interacting with the ClickHouse database.

Provides a client class for executing queries and batch data insertion.
"""

import logging
from clickhouse_driver import Client
from typing import List, Dict, Any


class ClickHouseDataLoader:
    """
    Wrapper class around `clickhouse-driver` to simplify ClickHouse operations.

    Provides methods for executing arbitrary queries, batch data insertion,
    and retrieving table schema.
    """

    def __init__(self, host: str, port: int, user: str, password: str = ""):
        """
        Initializes the client and establishes a connection.

        Args:
            host (str): ClickHouse server host.
            port (int): TCP port for the native protocol.
            user (str): Username.
            password (str): User password.
        """
        try:
            self.client = Client(host=host, port=port, user=user, password=password)
            self.client.execute('SELECT 1')
        except Exception as e:
            logging.error("Failed to connect to ClickHouse: %s", e)
            raise

    def execute_query(self, query: str, params: Any = None) -> list:
        """
        Executes an arbitrary query against ClickHouse.

        Args:
            query (str): SQL query to execute.
            params (Any, optional): Parameters for the query.

        Returns:
            list: Result of the query execution.
        """
        try:
            return self.client.execute(query, params)
        except Exception as e:
            logging.error("Query execution error '%s...': %s", query[:100], e)
            raise

    def insert_data(self, table_name: str, data: List[Dict]):
        """
        Performs batch insertion of data into the specified table.

        Args:
            table_name (str): Name of the target table.
            data (List[Dict]): A list of dictionaries, where each dictionary represents a row.

        Returns:
            None
        """
        if not data:
            logging.warning("No data to insert into table '%s'.", table_name)
            return

        columns = data[0].keys()
        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES"

        try:
            self.client.execute(insert_query, data)
        except Exception as e:
            logging.error("Error inserting data into table '%s': %s", table_name, e)
            raise

    def get_table_schema(self, table_name: str, database: str = 'default') -> list[dict]:
        """
        Retrieves the table schema (column names and types) from ClickHouse's system table.

        Args:
            table_name (str): Name of the table to retrieve the schema for.
            database (str, optional): Database name. Defaults to 'default'.

        Returns:
            list[dict]: A list of dictionaries, describing each column: {'name': str, 'type': str}.
        """
        query = "SELECT name, type FROM system.columns WHERE database = %(database)s AND table = %(table)s"
        params = {'database': database, 'table': table_name}
        result = self.execute_query(query, params)
        return [{'name': row[0], 'type': row[1]} for row in result]
