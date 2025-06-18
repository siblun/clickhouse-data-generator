"""
Модуль для парсинга и валидации файла конфигурации `config.json`.
"""

import json
import os
from typing import Any, Dict


class ConfigParser:
    """
    Класс для чтения и предоставления доступа к настройкам из JSON-файла.

    При инициализации загружает файл конфигурации и обеспечивает
    безопасные методы для доступа к его параметрам.
    """

    def __init__(self, config_path: str = 'config.json'):
        """
        Инициализирует парсер и загружает конфигурацию.

        Args:
            config_path (str): Путь к файлу конфигурации.
        """
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """
        Внутренний метод для загрузки и парсинга JSON-файла.

        Returns:
            Dict[str, Any]: Словарь с настройками.

        Raises:
            FileNotFoundError: Если файл конфигурации не найден.
            ValueError: Если файл имеет неверный JSON-формат.
        """
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Файл конфигурации не найден: {self.config_path}")
        with open(self.config_path, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError as e:
                raise ValueError(f"Ошибка парсинга JSON в файле {self.config_path}: {e}")

    def get_setting(self, key: str, default: Any = None, required: bool = False) -> Any:
        """
        Безопасно получает значение настройки по ключу.

        Args:
            key (str): Ключ настройки.
            default (Any, optional): Значение по умолчанию, если ключ отсутствует.
            required (bool, optional): Если True, вызывает ошибку, если ключ отсутствует.

        Returns:
            Any: Значение настройки.

        Raises:
            ValueError: Если `required=True` и ключ не найден.
        """
        value = self.config.get(key, default)
        if required and value is None:
            raise ValueError(f"Обязательная настройка '{key}' отсутствует в файле конфигурации.")
        return value

    def get_clickhouse_credentials(self) -> Dict[str, Any]:
        """Возвращает сгруппированные учетные данные для ClickHouse."""
        return {
            'host': self.get_setting('clickhouse_host', required=True),
            'port': self.get_setting('clickhouse_port', default=9000),
            'user': self.get_setting('clickhouse_user', required=True),
            'password': self.get_setting('clickhouse_password', default='')
        }

    def get_table_info(self) -> Dict[str, Any]:
        """Возвращает сгруппированную информацию о целевой таблице."""
        return {
            'name': self.get_setting('table_name', required=True),
            'schema_file_path': self.get_setting('schema_file_path'),
        }

    def get_generation_settings(self) -> Dict[str, Any]:
        """Возвращает сгруппированные настройки для процесса генерации данных."""
        return {
            'inserts_per_query': self.get_setting('inserts_per_query', default=1000),
            'total_inserts': self.get_setting('total_inserts', default=1000),
            'generation_seed': self.get_setting('generation_seed'),
            'hints': self.get_setting('hints', default={}),
        }
