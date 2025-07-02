"""
Module for generating test data based on a ClickHouse table schema.
"""

import random
import string
import logging
from datetime import datetime, timedelta, date
from typing import List, Dict, Any


class DataGenerator:
    """
    Class responsible for generating data that conforms to a table schema.

    Supports various ClickHouse data types, "hints" for controlling value ranges,
    and a seed for reproducibility.
    """

    def __init__(self, schema: List[Dict], hints: Dict = None, seed: int = None):
        """
        Initializes the generator.

        Args:
            schema (List[Dict]): Table schema, a list of dictionaries {'name': str, 'type': str}.
            hints (Dict, optional): "Hints" for value generation.
            seed (int, optional): Seed for the random number generator for reproducibility.
        """
        self.schema = schema
        self.hints = hints if hints is not None else {}
        self.rng = random.Random(seed)
        self._setup_type_generators()

    def _setup_type_generators(self):
        """
        Creates a mapping of basic ClickHouse types to generator functions.
        """
        self.type_generators = {
            'UInt8': lambda: self.rng.randint(0, 255),
            'UInt16': lambda: self.rng.randint(0, 65535),
            'UInt32': lambda: self.rng.randint(0, 4294967295),
            'UInt64': lambda: self.rng.randint(0, 18446744073709551615),
            'Int8': lambda: self.rng.randint(-128, 127),
            'Int16': lambda: self.rng.randint(-32768, 32767),
            'Int32': lambda: self.rng.randint(-2147483648, 2147483647),
            'Int64': lambda: self.rng.randint(-9223372036854775808, 9223372036854775807),
            'Float32': lambda: self.rng.uniform(-1e3, 1e3),
            'Float64': lambda: self.rng.uniform(-1e6, 1e6),
            'String': self._generate_string,
            'Date': self._generate_date,
            'DateTime': self._generate_datetime,
            'DateTime64': self._generate_datetime,
            'Bool': lambda: self.rng.choice([True, False]),
        }

    def _generate_string(self, length_min=5, length_max=15) -> str:
        """Generates a random string."""
        length = self.rng.randint(length_min, length_max)
        return ''.join(self.rng.choice(string.ascii_letters + string.digits) for _ in range(length))

    def _generate_date(self) -> date:
        """Generates a random date within the last year (by default)."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        return (start_date + timedelta(days=self.rng.randint(0, 365))).date()

    def _generate_datetime(self) -> datetime:
        """Generates a random datetime within the last year (by default)."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        delta_seconds = int((end_date - start_date).total_seconds())
        return start_date + timedelta(seconds=self.rng.randint(0, delta_seconds))

    def generate_row(self) -> Dict[str, Any]:
        """
        Generates one row of data as a dictionary.

        Iterates through each column in the schema, checks for a "hint",
        and generates the corresponding value.

        Returns:
            Dict[str, Any]: A dictionary representing one generated row.
        """
        row = {}
        for col in self.schema:
            col_name, col_type = col['name'], col['type']

            if col_name in self.hints:
                hint = self.hints[col_name]
                if isinstance(hint, list):
                    row[col_name] = self.rng.choice(hint)
                elif isinstance(hint, dict) and 'start' in hint and 'end' in hint:
                    start = datetime.fromisoformat(hint['start'])
                    end = datetime.fromisoformat(hint['end'])
                    delta = int((end - start).total_seconds())
                    gen_date = start + timedelta(seconds=self.rng.randint(0, delta))
                    row[col_name] = gen_date.date() if col_type == 'Date' else gen_date
                elif isinstance(hint, list) and len(hint) == 2:
                    if 'Float' in col_type:
                        row[col_name] = self.rng.uniform(hint[0], hint[1])
                    else:
                        row[col_name] = self.rng.randint(hint[0], hint[1])
                else:
                    logging.warning("Unknown format hint for '%s'. Generating by type.", col_name)
                    row[col_name] = self._generate_by_type(col_type)
            else:
                row[col_name] = self._generate_by_type(col_type)
        return row

    def generate_rows_batch(self, count: int) -> List[Dict]:
        """
        Generates a batch of 'count' rows.

        Args:
            count (int): The number of rows to generate for the batch.

        Returns:
            List[Dict]: A list of dictionaries, each representing a generated row.
        """
        batch = []
        for _ in range(count):
            batch.append(self.generate_row())
        return batch

    def _generate_by_type(self, col_type: str) -> Any:
        """
        Selects and calls the appropriate generator based on the base column type.

        Args:
            col_type (str): Full column type from the schema (e.g., `LowCardinality(String)`).

        Returns:
            Any: The generated value.
        """
        base_type = col_type.split('(')[0]
        generator = self.type_generators.get(base_type)

        if generator:
            return generator()
        else:
            logging.warning("Unknown column type '%s'. Return NULL.", col_type)
            return None
