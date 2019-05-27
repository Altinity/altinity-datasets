# Copyright (c) 2019 Altinity LTD
#
# This product is licensed to you under the
# Apache License, Version 2.0 (the "License").
# You may not use this product except in compliance with the License.
#
# This product may include a number of subcomponents with
# separate copyright notices and license terms. Your use of the source
# code for the these subcomponents is subject to the terms and
# conditions of the subcomponent's license, as noted in the LICENSE file.

import glob
import logging
import os
import re

from clickhouse_driver import Client

"""Implements a driver module for ClickHouse that encapsulates driver
   API calls and SQL execution conventions"""

# Define logger
logger = logging.getLogger(__name__)


class TableData:
    """Metadata for a table in Clickhouse"""
    def __init__(self, database, name, partition_key=None,
                 sorting_key=None):
       self.database = database
       self.name = name
       self.partition_key = partition_key
       self.sorting_key = sorting_key
       self.create_table = None

class ClientWrapper:
    """Context manager to allow use of ClickHouse connections in with clause"""
    def __init__(self, *args, **kwargs):
        self.client = Client(*args, **kwargs)

    def __enter__(self):
        return self.client

    def __exit__(self, *args):
        """Disconnect from server to up free socket"""
        self.client.disconnect()


class ClickHouse:
    """Connector class for ClickHouse data warehouse operations"""
    def __init__(self, host='local_host', database='default'):
        """Set up the connector
        :param host: (str): ClickHouse server host 
        :param database: (str): Database 
        """
        self.host = host
        self.database = database

    def fetch_tables(self, table_regex=None):
        """Fetch table metadata
        :param table_regex: (str): Regex to select tables
        :return: A list of Table instances
        """
        # Connect to database and fetch tables. 
        logger.info("Fetch tables from host: {0} database: {1}".format(self.host, self.database))
        with ClientWrapper(self.host) as client:
            table_query = "select name, partition_key, sorting_key from system.tables where database='{0}' and engine not like 'Materialized%' and name not like '.%'"
            result, types = client.execute(
                table_query.format(self.database), 
                with_column_types=True)
            tables = []
            for row in result:
                name = row[0]
                if table_regex is not None:
                    if not re.search(table_regex, name):
                        continue
                partition_key = row[1]
                sorting_key = row[2]
                table = TableData(self.database, name, partition_key, sorting_key)
                tables.append(table)

        # Add CREATE TABLE definitions. 
        with ClientWrapper(self.host, database=self.database) as client:
            for table in tables:
                # Fetch CREATE TABLE definition.
                show_create_query = "show create table {0}"
                result = client.execute(show_create_query.format(table.name))
                for row in result:
                    # Remove the database name. 
                    pattern = re.compile('CREATE TABLE {0}\\.'.format(self.database))
                    table.create_table = pattern.sub('CREATE TABLE ', row[0])

        return tables

    def fetch_row_count(self, table):
        """Return number of rows in table
        :param table: (TableData): TableData instance with table data
        :return: Count of row
        """
        with ClientWrapper(self.host, database=table.database) as client:
            sql = "SELECT count(*) FROM {0}.{1}".format(table.database, table.name)
            count, _ = self._select_scalar(client, sql)
            return count

    def fetch_partitions(self, table, format="CSVWithNames"):
        """Return partition keys and SQL statement to fetch them
        :param table: (TableData): TableData instance with table data
        :param format: (str): Output format
        :param collapse_partitions: If True merge small tables to one partition
        :return: Array of tuple(partition_key, sql_statement). If table is collapsed or has no partitions the partition key will be None
        """
        partition_list = []

        # If there is no partition key, return a select on all data including 
        # a sort order if key is available. 
        if table.partition_key is None:
            sql = "SELECT * FROM {0}.{1}".format(table.database, table.name)
            if table.sorting_key is not None:
                sql += " ORDER BY {0}".format(table.sorting_key)
            sql += " FORMAT {0}".format(format)
            partition_list.append((None, sql))
        else:
            # Fetch keys and generate the type format. String types require
            # quotes. 
            with ClientWrapper(self.host, database=table.database) as client:
                partition_sql = "SELECT {0} AS key FROM {1}.{2} GROUP BY key ORDER BY key".format(table.partition_key, table.database, table.name)
                partition_keys, partition_key_type = self._select_array(client, partition_sql)

            if partition_key_type == "String":
                key_fmt = "'{0}'"
            else:
                key_fmt = "{0}"

            # Construct select template for partitioned data. 
            partition_sql = "SELECT * FROM {0}.{1}".format(table.database, table.name)
            partition_sql += " WHERE {0} = {1}".format(table.partition_key, key_fmt)
            if table.sorting_key is not None:
                partition_sql += " ORDER BY {0}".format(table.sorting_key)
            partition_sql += " FORMAT {0}".format(format)

            # Generate SQL statements. 
            for key in partition_keys:
                if partition_key_type == "String":
                    key = key.replace("'", "\\'")
                partition_list.append((key, partition_sql.format(key)))

        return partition_list

    def execute(self, sql, verbose=False, dry_run=False):
        """Execute a SQL query"""
        with ClientWrapper(self.host, database=self.database) as client:
            if verbose:
                logger.debug("SQL: {0}".format(sql))
            if not dry_run:
                return client.execute(sql)

    def _select_array(self, conn, sql):
        """Return select on a single column as an array
        :param table: (TableData): TableData instance with table data
        :param sql: (str): Query
        :return: Tuple with two elements: array of values and SQL type
        """
        result, types = conn.execute(sql, with_column_types=True)
        array = []
        for row in result:
            array.append(row[0])
        # Type value is 1st value in list of tuples, 2nd value in the tuple.
        return array, types[0][1]

    def _select_scalar(self, conn, sql):
        """Return single value from select
        :param table: (TableData): TableData instance with table data
        :param sql: (str): Query
        :return: Tuple with two elements: scalar value and SQL type
        """
        result, types = conn.execute(sql, with_column_types=True)
        array = []
        for row in result:
            return row[0], types[0][1]
