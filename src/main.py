"""
Главный исполняемый файл для запуска процесса генерации и вставки данных.

Этот скрипт выполняет следующие шаги:
1. Читает конфигурацию из файла `config.json`.
2. Устанавливает соединение с ClickHouse.
3. Определяет схему целевой таблицы (либо из SQL-файла, либо напрямую из БД).
4. Инициализирует генератор данных на основе схемы и "подсказок" из конфига.
5. В цикле генерирует данные и вставляет их в таблицу пачками (batch).
6. Выводит в консоль лог о ходе выполнения.
"""

import logging
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.config_parser import ConfigParser
from src.schema_parser import SchemaParser
from src.data_generator import DataGenerator
from src.clickhouse_client import ClickHouseDataLoader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def main():
    """
    Основная функция, управляющая процессом генерации данных.
    """
    try:
        config_path = 'config.json'
        if not os.path.exists(config_path):
            config_path = os.path.join(os.path.dirname(__file__), '..', config_path)

        logging.info("Loading configuration from %s...", config_path)
        config_parser = ConfigParser(config_path)
        ch_creds = config_parser.get_clickhouse_credentials()
        table_info = config_parser.get_table_info()
        gen_settings = config_parser.get_generation_settings()

        table_name = table_info['name']
        schema_file_path = table_info['schema_file_path']
        total_inserts = gen_settings['total_inserts']
        inserts_per_query = gen_settings['inserts_per_query']
        generation_seed = gen_settings['generation_seed']
        hints = gen_settings['hints']

        logging.info("Configuration loaded. Table: %s, Total records to insert: %d", table_name, total_inserts)

        logging.info("Connecting to ClickHouse at %s:%d...", ch_creds['host'], ch_creds['port'])
        clickhouse_loader = ClickHouseDataLoader(
            host=ch_creds['host'],
            port=ch_creds['port'],
            user=ch_creds['user'],
            password=ch_creds['password'],
        )

        schema = []
        schema_parser = SchemaParser(clickhouse_loader.client)

        if schema_file_path:
            full_schema_path = os.path.abspath(os.path.join(os.path.dirname(config_path), schema_file_path))
            if os.path.exists(full_schema_path):
                logging.info("Attempting to parse schema from file: %s", full_schema_path)
                schema = schema_parser.parse_schema_from_sql_file(full_schema_path)

        if not schema:
            logging.info("Attempting to retrieve schema for table '%s' from ClickHouse...", table_name)
            schema = schema_parser.get_schema_from_clickhouse(table_name)

        if not schema:
            raise ValueError(
                "Failed to retrieve table schema from neither file nor ClickHouse."
                "Ensure the table exists or the path to the SQL file is correct."
            )

        logging.info("Table schema '%s' successfully retrieved.", table_name)

        data_generator = DataGenerator(schema, hints=hints, seed=generation_seed)
        logging.info("Starting generation and insertion of %d rows...", total_inserts)

        generated_rows_count = 0
        current_batch = []

        for i in range(total_inserts):
            row = data_generator.generate_row()
            current_batch.append(row)

            if len(current_batch) >= inserts_per_query or (i == total_inserts - 1 and current_batch):
                clickhouse_loader.insert_data(table_name, current_batch)
                generated_rows_count += len(current_batch)
                logging.info("Inserted %d/%d rows...", generated_rows_count, total_inserts)
                current_batch = []

        logging.info("--- Data generation and insertion completed successfully! ---")

    except FileNotFoundError as e:
        logging.error("Critical error: File not found. %s", e)
    except KeyError as e:
        logging.error("Configuration error: Missing required key %s in config.json.", e)
    except ValueError as e:
        logging.error("Configuration or schema error: %s", e)
    except Exception as e:
        logging.critical("An unexpected error occurred: %s", e, exc_info=True)


if __name__ == '__main__':
    main()
