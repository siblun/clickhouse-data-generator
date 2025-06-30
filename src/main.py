import logging
import os
import re
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.config_parser import ConfigParser
from src.schema_parser import SchemaParser
from src.data_generator import DataGenerator
from src.clickhouse_client import ClickHouseDataLoader
from clickhouse_driver.errors import ServerException

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def main():
    """
    Main function controlling the data generation process.
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

        schema_parser = SchemaParser(clickhouse_loader.client)

        if schema_file_path:
            full_schema_file_path = os.path.join(os.path.dirname(__file__), '..', schema_file_path)
            if not os.path.exists(full_schema_file_path):
                raise FileNotFoundError(f"Schema file not found at path: {full_schema_file_path}. "
                                        "Cannot create table from file.")

            logging.info(f"Attempting to create table '{table_name}' from schema file: {full_schema_file_path}")
            with open(full_schema_file_path, 'r', encoding='utf-8') as f:
                create_table_sql = f.read()

            create_table_sql_if_not_exists = re.sub(
                r'CREATE\s+TABLE\s+(\S+)',
                r'CREATE TABLE IF NOT EXISTS \1',
                create_table_sql,
                flags=re.IGNORECASE
            )
            try:
                clickhouse_loader.execute_query(create_table_sql_if_not_exists)
                logging.info(f"Table '{table_name}' successfully created or already exists.")
            except ServerException as e:
                if "already exists" in str(e):
                    logging.info(f"Table '{table_name}' already exists. Continuing.")
                else:
                    logging.error(f"ClickHouse error during table creation for '{table_name}': %s", e)
                    raise
            except Exception as e:
                logging.error(f"Unexpected error during table creation attempt for '{table_name}': %s", e)
                raise
        else:
            logging.warning(
                "Schema file path is not specified in config.json. Table creation from file will be skipped. "
                "Ensure the table exists in ClickHouse before running data generation.")

        logging.info(f"Retrieving schema for table '{table_name}' from ClickHouse...")
        schema = schema_parser.get_schema_from_clickhouse(table_name)

        if not schema:
            raise ValueError(
                f"Failed to retrieve table schema from ClickHouse. "
                "Ensure the table exists and is accessible in ClickHouse."
            )

        logging.info(f"Table schema for '{table_name}' successfully retrieved:")
        for col in schema:
            logging.info(f"  - {col['name']}: {col['type']}")

        data_generator = DataGenerator(schema, hints=hints, seed=generation_seed)

        logging.info("Starting generation and insertion of %d rows...", total_inserts)

        generated_rows_count = 0
        while generated_rows_count < total_inserts:
            rows_to_generate_in_batch = min(inserts_per_query, total_inserts - generated_rows_count)

            current_batch = data_generator.generate_rows_batch(rows_to_generate_in_batch)

            if current_batch:
                clickhouse_loader.insert_data(table_name, current_batch)
                generated_rows_count += len(current_batch)
                logging.info("  Inserted %d/%d rows...", generated_rows_count, total_inserts)

        logging.info("--- Data generation and insertion completed successfully! ---")
        logging.info(f"Total %d rows inserted into table '{table_name}'.", generated_rows_count)

    except FileNotFoundError as e:
        logging.error("Critical error: File not found. %s", e)
        logging.error("Please ensure the configuration file and/or schema file exist at the specified paths.")
    except ValueError as e:
        logging.error("Configuration or schema error: %s", e)
    except Exception as e:
        logging.critical("An unexpected error occurred: %s", e, exc_info=True)


if __name__ == '__main__':
    main()
