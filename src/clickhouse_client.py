"""
Модуль для взаимодействия с базой данных ClickHouse.

Предоставляет класс-клиент для выполнения запросов и пакетной вставки данных.
"""

import logging
from clickhouse_driver import Client
from typing import List, Dict, Any


class ClickHouseDataLoader:
    """
    Класс-обертка над `clickhouse-driver` для упрощения операций с ClickHouse.

    Предоставляет методы для выполнения произвольных запросов, пакетной вставки
    данных и получения схемы таблицы.
    """

    def __init__(self, host: str, port: int, user: str, password: str = ""):
        """
        Инициализирует клиент и устанавливает соединение.

        Args:
            host (str): Хост сервера ClickHouse.
            port (int): TCP-порт для нативного протокола.
            user (str): Имя пользователя.
            password (str): Пароль пользователя.
        """
        try:
            self.client = Client(host=host, port=port, user=user, password=password)
            self.client.execute('SELECT 1')
        except Exception as e:
            logging.error("Не удалось подключиться к ClickHouse: %s", e)
            raise

    def execute_query(self, query: str, params: Any = None) -> list:
        """
        Выполняет произвольный запрос к ClickHouse.

        Args:
            query (str): SQL-запрос для выполнения.
            params (Any, optional): Параметры для запроса.

        Returns:
            list: Результат выполнения запроса.
        """
        try:
            return self.client.execute(query, params)
        except Exception as e:
            logging.error("Ошибка выполнения запроса '%s...': %s", query[:100], e)
            raise

    def insert_data(self, table_name: str, data: List[Dict]):
        """
        Выполняет пакетную (batch) вставку данных в указанную таблицу.

        Args:
            table_name (str): Имя целевой таблицы.
            data (List[Dict]): Список словарей, где каждый словарь представляет строку.

        Returns:
            None
        """
        if not data:
            logging.warning("Нет данных для вставки в таблицу '%s'.", table_name)
            return

        columns = data[0].keys()
        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES"

        try:
            self.client.execute(insert_query, data)
        except Exception as e:
            logging.error("Ошибка при вставке данных в таблицу '%s': %s", table_name, e)
            raise

    def get_table_schema(self, table_name: str, database: str = 'default') -> list[dict]:
        """
        Получает схему таблицы (имена и типы колонок) из системной таблицы ClickHouse.

        Args:
            table_name (str): Имя таблицы для получения схемы.
            database (str, optional): Имя базы данных. По умолчанию 'default'.

        Returns:
            list[dict]: Список словарей, описывающих каждую колонку: {'name': str, 'type': str}.
        """
        query = "SELECT name, type FROM system.columns WHERE database = %(database)s AND table = %(table)s"
        params = {'database': database, 'table': table_name}
        result = self.execute_query(query, params)
        return [{'name': row[0], 'type': row[1]} for row in result]
