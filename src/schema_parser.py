"""
Module for determining ClickHouse table schema.

Supports retrieving the schema directly from ClickHouse's system tables.
"""
import logging
import re
from typing import List, Dict

from clickhouse_driver import Client


class SchemaParser:
    """
    Class encapsulating the logic for retrieving table schema.
    """

    def __init__(self, client: Client):
        """
        Initializes the parser.

        Args:
            client (Client): An active `clickhouse-driver` client for database interaction.
        """
        self.client = client


    def get_schema_from_clickhouse(self, table_name: str, database: str = "default") -> List[Dict]:
        """
        Retrieves the table schema directly from ClickHouse's `system.columns` table.
        This is the most reliable method.

        Args:
            table_name (str): The name of the table.
            database (str, optional): The name of the database.

        Returns:
            List[Dict]: A list of dictionaries describing the columns.
        """
        try:
            query = "SELECT name, type FROM system.columns WHERE database = %(database)s AND table = %(table)s"
            params = {'database': database, 'table': table_name}
            result = self.client.execute(query, params)
            return [{'name': row[0], 'type': row[1]} for row in result]
        except Exception as e:
            logging.error("Error retrieving schema from ClickHouse for table '%s': %s", table_name, e)
            return []
