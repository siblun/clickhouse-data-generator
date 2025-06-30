import os
import sys
import re
import time
import logging
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.config_parser import ConfigParser
from src.schema_parser import SchemaParser
from src.data_generator import DataGenerator
from src.clickhouse_client import ClickHouseDataLoader
from clickhouse_driver.errors import ServerException

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def main():
    """
    Основная функция, управляющая процессом генерации данных.
    """
    try:
        config_path = 'config.json'
        if not os.path.exists(config_path):
            config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')

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

        table_exists = False
        max_retries = 5
        retry_delay = 2

        for attempt in range(max_retries):
            logging.info(f"Attempt {attempt + 1}/{max_retries}: Checking existence of table '{table_name}'...")
            try:
                current_schema = schema_parser.get_schema_from_clickhouse(table_name)
                if current_schema:
                    table_exists = True
                    schema = current_schema
                    logging.info(f"Table '{table_name}' found in ClickHouse. Using schema from DB.")
                    break
                else:
                    logging.info(f"Table '{table_name}' not found. Attempting to create...")
                    if schema_file_path:
                        full_schema_file_path = os.path.join(os.path.dirname(__file__), '..', schema_file_path)
                        if os.path.exists(full_schema_file_path):
                            with open(full_schema_file_path, 'r', encoding='utf-8') as f:
                                create_table_sql = f.read()
                            create_table_sql_if_not_exists = re.sub(
                                r'CREATE\s+TABLE\s+(\S+)',
                                r'CREATE TABLE IF NOT EXISTS \1',
                                create_table_sql,
                                flags=re.IGNORECASE
                            )
                            clickhouse_loader.execute_query(create_table_sql_if_not_exists)
                            logging.info(f"Table '{table_name}' successfully created (or already existed) from file.")
                            schema = schema_parser.get_schema_from_clickhouse(table_name)
                            if schema:
                                table_exists = True
                                logging.info(f"Table schema for '{table_name}' successfully retrieved after creation.")
                                break
                            else:
                                logging.warning(f"Table '{table_name}' created, but schema not retrieved. Retrying.")
                        else:
                            logging.warning(f"Schema file not found at path: {full_schema_file_path}. Cannot create table.")
                    else:
                        logging.warning("Schema file path not specified. Cannot create table.")

            except ServerException as e:
                if "already exists" in str(e):
                    logging.info(f"Table '{table_name}' already exists. Continuing.")
                    table_exists = True
                    schema = schema_parser.get_schema_from_clickhouse(table_name)
                    break
                else:
                    logging.error(f"ClickHouse error during table creation/check for '{table_name}': {e}")
            except Exception as e:
                logging.error(f"Unexpected error during table creation/check attempt for '{table_name}': {e}")

            if not table_exists and attempt < max_retries - 1:
                logging.info(f"Waiting {retry_delay} seconds before next attempt...")
                time.sleep(retry_delay)

        if not table_exists or not schema:
            raise ValueError(
                "Failed to create or retrieve table schema from ClickHouse after multiple attempts. "
                "Ensure ClickHouse is running, accessible, and the SQL schema file is correct."
            )

        logging.info(f"Table schema for '{table_name}' successfully retrieved:")
        for col in schema:
            print(f"  - {col['name']}: {col['type']}")

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
                logging.info("  Inserted %d/%d rows...", generated_rows_count, total_inserts)
                current_batch = []
        logging.info("--- Data generation and insertion completed successfully! ---")
        logging.info(f"Total %d rows inserted into table '{table_name}'.", generated_rows_count)

    except FileNotFoundError as e:
        logging.error("Critical error: File not found. %s", e)
        print("Пожалуйста, убедитесь, что файл конфигурации и/или файл схемы существуют по указанным путям.")
    except ValueError as e:
        logging.error("Configuration or schema error: %s", e)
    except Exception as e:
        logging.critical("An unexpected error occurred: %s", e, exc_info=True)


if __name__ == '__main__':
    main()
