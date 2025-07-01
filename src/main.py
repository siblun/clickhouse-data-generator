"""
Main executable file for running the data generation and insertion process.

This script performs the following steps:
1. Reads configuration from the `config.json` file.
2. Establishes a connection to ClickHouse.
3. Dynamically creates the target table from definition in config.json if specified,
   or ensures it exists.
4. Determines the schema of the target table directly from ClickHouse.
5. Initializes the data generator based on the schema and "hints" from the config.
6. Generates data in batches and inserts them into the table.
7. Outputs logging messages about the execution progress.
"""

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
        schema_file_path = table_info.get('schema_file_path')
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

        logging.info(f"Retrieving schema for table '{table_name}' from ClickHouse...")
        schema = schema_parser.get_schema_from_clickhouse(table_name)

        if not schema:
            raise ValueError(
                f"Failed to retrieve table schema for '{table_name}' from ClickHouse. "
                "The table might not exist or is inaccessible. This tool requires the table to pre-exist."
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
