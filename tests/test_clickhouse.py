#!/usr/bin/python3

"""Tests behavior of page location indexes"""
import logging
import os
import unittest

import altinity_datasets

from altinity_datasets import clickhouse

# Define logger
logger = logging.getLogger(__name__)


class ClickHouseTest(unittest.TestCase):
    def setUp(self):
        self.host = "localhost"
        self.db = "default"
        self.table = "^iris$"
        self.ch = clickhouse.ClickHouse(host=self.host, database=self.db)

    def test_fetch_tables(self):
        """Fetch table definitions"""
        tables = self.ch.fetch_tables()
        self.assertIsNotNone(tables)
        self.assertTrue(len(tables) > 0)

    def test_fetch_tables_with_regex(self):
        """Fetch table definitions using regex selector"""
        tables = self.ch.fetch_tables(table_regex=self.table)
        self.assertIsNotNone(tables)
        self.assertEqual(1, len(tables))

    def test_fetch_partition(self):
        """Fetch partition keys from table"""
        tables = self.ch.fetch_tables(table_regex=self.table)
        partitions = self.ch.fetch_partitions(tables[0])
        logger.info("PARTITION KEYS: " + str(partitions))
        self.assertIsNotNone(partitions)
        self.assertTrue(len(partitions) > 0)

if __name__ == '__main__':
    unittest.main()
