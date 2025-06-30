"""
Module for parsing and validating the `config.json` configuration file.
"""

import json
import os
from typing import Any, Dict


class ConfigParser:
    """
    Class for reading and providing access to settings from a JSON file.

    Upon initialization, it loads the configuration file and provides
    safe methods for accessing its parameters.
    """

    def __init__(self, config_path: str = 'config.json'):
        """
        Initializes the parser and loads the configuration.

        Args:
            config_path (str): Path to the configuration file.
        """
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """
        Internal method for loading and parsing the JSON file.

        Returns:
            Dict[str, Any]: A dictionary with settings.

        Raises:
            FileNotFoundError: If the configuration file is not found.
            ValueError: If the file has an invalid JSON format.
        """
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        with open(self.config_path, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError as e:
                raise ValueError(f"JSON parsing error in file {self.config_path}: {e}")

    def get_setting(self, key: str, default: Any = None, required: bool = False) -> Any:
        """
        Safely retrieves a setting's value by key.

        Args:
            key (str): The setting key.
            default (Any, optional): The default value if the key is missing.
            required (bool, optional): If True, raises an error if the key is missing.

        Returns:
            Any: The setting's value.

        Raises:
            ValueError: If `required=True` and the key is not found.
        """
        value = self.config.get(key, default)
        if required and value is None:
            raise ValueError(f"Required setting '{key}' is missing in the configuration file.")
        return value

    def get_clickhouse_credentials(self) -> Dict[str, Any]:
        """Returns grouped ClickHouse credentials."""
        return {
            'host': self.get_setting('clickhouse_host', required=True),
            'port': self.get_setting('clickhouse_port', default=9000),
            'user': self.get_setting('clickhouse_user', required=True),
            'password': self.get_setting('clickhouse_password', default='')
        }

    def get_table_info(self) -> Dict[str, Any]:
        """Returns grouped information about the target table."""
        return {
            'name': self.get_setting('table_name', required=True),
            'schema_file_path': self.get_setting('schema_file_path'),
        }

    def get_generation_settings(self) -> Dict[str, Any]:
        """Returns grouped settings for the data generation process."""
        return {
            'inserts_per_query': self.get_setting('inserts_per_query', default=10),
            'total_inserts': self.get_setting('total_inserts', default=10),
            'generation_seed': self.get_setting('generation_seed'),
            'hints': self.get_setting('hints', default={}),
        }

    def get_table_definition(self) -> Dict[str, Any]:
        """
        Returns the full table definition for dynamic CREATE TABLE statement.
        """
        return self.get_setting('table_definition', required=False, default=None)

