"""
Модуль для определения схемы таблицы ClickHouse.

Поддерживает два способа получения схемы:
1. Парсинг `CREATE TABLE` из предоставленного SQL-файла.
2. Запрос к системным таблицам ClickHouse.
"""
import logging
import re
from typing import List, Dict

from clickhouse_driver import Client


class SchemaParser:
    """
    Класс, инкапсулирующий логику получения схемы таблицы.
    """

    def __init__(self, client: Client):
        """
        Инициализирует парсер.

        Args:
            client (Client): Активный клиент `clickhouse-driver` для взаимодействия с БД.
        """
        self.client = client

    def parse_schema_from_sql_file(self, file_path: str) -> List[Dict]:
        """
        Парсит `CREATE TABLE` DDL из файла и извлекает имена и типы колонок.

        Примечание:
            Этот метод использует регулярные выражения и может быть ненадёжным
            для сложных SQL-схем (например, с `DEFAULT`, `CODEC`, комментариями).
            Для продакшн-использования рекомендуется получать схему напрямую из БД.

        Args:
            file_path (str): Абсолютный путь к `.sql` файлу.

        Returns:
            List[Dict]: Список словарей, описывающих колонки, или пустой список при ошибке.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                sql_ddl = file.read()

            match = re.search(r'CREATE\s+TABLE\s+\S+\s*\((.+)\)\s*ENGINE', sql_ddl, re.DOTALL | re.IGNORECASE)
            if not match:
                logging.warning("Не удалось найти блок 'CREATE TABLE (...)' в SQL файле: %s", file_path)
                return []

            columns_str = match.group(1)
            columns_str = re.sub(r'--.*', '', columns_str)
            columns_str = re.sub(r'/\*.*?\*/', '', columns_str, flags=re.DOTALL)

            column_defs = re.findall(r'^\s*`?(\w+)`?\s+([\w\(\),\'\s]+?)(?:\s+DEFAULT.*|,|\s*\n)', columns_str,
                                     re.MULTILINE)

            columns = []
            for col_name, col_type_full in column_defs:
                col_type = col_type_full.strip().split()[0]
                columns.append({'name': col_name, 'type': col_type})

            return columns
        except FileNotFoundError:
            logging.error("Файл схемы не найден по пути: %s", file_path)
            return []
        except Exception as e:
            logging.error("Ошибка при парсинге SQL-файла схемы '%s': %s", file_path, e)
            return []

    def get_schema_from_clickhouse(self, table_name: str, database: str = "default") -> List[Dict]:
        """
        Получает схему таблицы напрямую из системной таблицы `system.columns`.
        Это наиболее надежный способ.

        Args:
            table_name (str): Имя таблицы.
            database (str, optional): Имя базы данных.

        Returns:
            List[Dict]: Список словарей, описывающих колонки.
        """
        try:
            query = "SELECT name, type FROM system.columns WHERE database = %(database)s AND table = %(table)s"
            params = {'database': database, 'table': table_name}
            result = self.client.execute(query, params)
            return [{'name': row[0], 'type': row[1]} for row in result]
        except Exception as e:
            logging.error("Ошибка при получении схемы из ClickHouse для таблицы '%s': %s", table_name, e)
            return []
