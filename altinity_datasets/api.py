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
import urllib.parse

import yaml

from altinity_datasets.proc_pool import ProcessPool
from altinity_datasets import clickhouse

# Define logger
logger = logging.getLogger(__name__)

# Base directory of the installation.
BASE = os.path.join(os.path.dirname(__file__), '..')

# A list of built-in repo locations.
BUILT_INS = [
    {
        'name': 'built-ins',
        'description': 'Built-in dataset repository',
        'path': os.path.realpath(os.path.join(BASE, 'altinity_datasets/built-ins'))
    },
]


def _sql(conn, sql, verbose=False, dry_run=False):
    """Execute a SQL command"""
    if verbose:
        logger.debug("SQL: {0}".format(sql))
    if not dry_run:
        conn.execute(sql)


def repos():
    """List known repos"""
    return BUILT_INS


def dataset_search(name, repo_path=None):
    """Search for dataset(s)
    :param name: (str): If specified show only datasets that match this name
    :param repo_path: (str): A path to the repo directory.  If specified 
                             search this repo path.  Otherwise search built-ins
    """
    datasets = []
    if repo_path is None:
        search_list = [os.path.join(BASE, repo['path']) for repo in BUILT_INS]
    else:
        search_list = [repo_path]

    for dir in search_list:
        children = [os.path.join(dir, child) for child in os.listdir(dir)]
        for child in children:
            manifest_yaml = os.path.join(child, 'manifest.yaml')
            # Skip if not a directory or if the name does not match. 
            if not os.path.isdir(child):
                continue
            elif name and name != os.path.basename(child):
                continue
            # Load the manifest if it exists. 
            if os.path.exists(manifest_yaml):
                with open(manifest_yaml, "r") as f:
                    manifest = yaml.safe_load(f)
            else:
                manifest = {}
            # Fill in location fields. 
            manifest['repo'] = os.path.basename(dir)
            manifest['path'] = child
            manifest['name'] = os.path.basename(child)
            datasets.append(manifest)

    return datasets


def dataset_load(name,
         repo_path=None,
         host='localhost',
         database=None,
         parallel=5,
         clean=False,
         verbose=False,
         dry_run=False,
         progress_reporter=None):
    """Load a dataset to database
    :param name: (str): Name of dataset
    :param repo_path: (str): Repo directory or None to search built-ins
    :param host: (str): ClickHouse server host 
    :param database: (str): Database (defaults to dataset name)
    :param parallel: (int): Number of processes to run in parallen when loading
    :param clean: (boolean): If True wipe out existing data
    :param dry_run: (boolean): If True print commands instead of executing them
    :param progress_reporter: (function): If specified call function with string message showing progress
    """
    # Look up the dataset.
    datasets = dataset_search(name, repo_path=repo_path)
    if len(datasets) == 0:
        raise Exception("Dataset not found: {0}".format(name))
    elif len(datasets) > 1:
        raise Exception(
            "Dataset name is ambiguous, must specify repo path: {0}"
            .format(name))
    else:
        dataset = datasets[0]

    # Use name as the database unless overridden by caller. 
    database = name if database is None else database
    logger.info("Loading to host: {0} database: {1}".format(host, database))

    # Clear database if requested. This connection cannot use the database
    # as it might not exist yet. 
    ch_0 = clickhouse.ClickHouse(host)
    if clean:
        _progress_and_info(
            "Dropping database if it exists: {0}".format(database),
            progress_reporter)
        ch_0.execute(
            "DROP DATABASE IF EXISTS {0}".format(database),
            dry_run=dry_run)

    # Create database.
    _progress_and_info(
        "Creating database if it does not exist: {0}".format(database),
        progress_reporter)
    ch_0.execute("CREATE DATABASE IF NOT EXISTS {0}".format(database),
         dry_run)

    # We can now safely reference the database.
    ch = clickhouse.ClickHouse(host, database=database)

    # Load table definitions in sequence.
    ddl_path = os.path.join(dataset['path'], "ddl")
    for sql_file in glob.glob(ddl_path + "/*"):
        _progress_and_info(
            "Executing DDL: {0}".format(sql_file), 
            progress_reporter)
        with open(sql_file, 'r') as f:
            script = f.read()
        ch.execute(script, dry_run=dry_run)

    # Build options for the clickhouse-client. 
    opts = ''
    if host:
        opts += " --host={0}".format(host)
    if database:
        opts += " --database={0}".format(database)

    # Define load scripts for each CSV load file.
    data_path = os.path.join(dataset['path'], "data")
    load_operations = []
    for table_dir in glob.glob(data_path + "/*"):
        logger.info("Processing table data: {0}".format(table_dir))
        table = os.path.basename(table_dir)
        load_sql = "INSERT INTO {0} FORMAT CSVWithNames".format(table)
        csv_files = glob.glob(table_dir + "/*csv*")
        for csv_file in sorted(csv_files):
            if re.match(r'^.*csv\.gz', csv_file):
                cat_cmd = "gzip -d -c {0}".format(csv_file)
            elif re.match(r'^.*csv', csv_file):
                cat_cmd = "cat {0}".format(csv_file)
            else:
                raise (
                    "Unable to open this type of file: {0}".format(csv_file))

            client_cmd = (
                "clickhouse-client{0} --query='{1}'".
                format(opts, load_sql))

            load_command = cat_cmd + " | " + client_cmd
            load_operations.append(
                (table, os.path.basename(csv_file), load_command))

    # Execute the load commands.
    pool = ProcessPool(size=parallel, dry_run=dry_run)
    for name, csv_file, cmd in load_operations:
        _progress_and_info(
            "Loading data: table={0}, file={1}".format(name, csv_file),
            progress_reporter)
        pool.exec(cmd)
    pool.drain()
    logger.info(pool.outputs)
    _progress_and_info(
        "Operation summary: succeeded={0}, failed={1}".format(pool.succeeded, pool.failed),
        progress_reporter)

def dataset_dump(name,
         repo_path='.',
         host='localhost',
         database=None,
         table_regex=None,
         parallel=5,
         overwrite=False,
         compress=True,
         verbose=False,
         dry_run=False,
         progress_reporter=None):
    """Dump a live dataset to file representation
    :param name: (str): Name of dataset
    :param repo_path: (str): Repo path (defaults to current dir)
    :param host: (str): ClickHouse server host 
    :param database: (str): Database (defaults to dataset name)
    :param table_regex: (str): Regex to select tables
    :param parallel: (int): Number of processes to run in parallel when dumping
    :param overwrite: (boolean): If True wipe out existing data
    :param compress: (boolean): If True compress data files
    :param dry_run: (boolean): If True print commands instead of executing them
    :param progress_reporter: (function): If specified call function with string message showing progress
    """
    # Connect to database and fetch table metadata. 
    database = name if database is None else database
    ch = clickhouse.ClickHouse(host, database=database)

    # Fetch tables to dump. 
    tables = ch.fetch_tables(table_regex=table_regex)
    if len(tables) == 0:
        raise("No tables found") 

    # Create the dataset directory. 
    if repo_path:
        ds_path = os.path.join(repo_path, name)
    else:
        ds_path = name
    ddl_path = os.path.join(ds_path, 'ddl')
    data_path = os.path.join(ds_path, 'data')
    _progress_and_info(
        "Preparing dataset directory: {0}".format(ds_path), 
        progress_reporter)
    os.makedirs(ds_path, exist_ok=overwrite)
    os.makedirs(ddl_path, exist_ok=overwrite)
    os.makedirs(data_path, exist_ok=overwrite)

    # Write a draft manifest with location fields filled in and others 
    # with default values. 
    manifest = {}
    manifest['title'] = "{0} Data Set".format(name)
    manifest['description'] = "Data set dumped from host {0}, database {1}".format(host, database)
    manifest['sources'] = "(Add source URL here)"

    # Compute size of the dataset by scanning tables. 
    _progress_and_info(
        "Computing data set size",
        progress_reporter)
    size = 0
    for table in tables:
        logger.info('Counting table rows: {0}'.format(table.name))
        table_rows = ch.fetch_row_count(table)
        _progress_and_info(
            'Table: {0} Rows: {1}'.format(table.name, table_rows),
            progress_reporter)
        size += table_rows
    manifest['size'] = '{0} rows'.format(size)
    _progress_and_info(
        'Total rows: {0}'.format(size), 
        progress_reporter)

    # Write the completed manifest. 
    manifest_yaml = os.path.join(ds_path, "manifest.yaml")
    _progress_and_info(
        "Writing manifest: {0}".format(manifest_yaml), 
        progress_reporter)
    with open(manifest_yaml, "w") as f:
         yaml.dump(manifest, f)

    # Dump each CREATE TABLE statement to a file with the same name. 
    for table in tables:
        sql_path = os.path.join(ddl_path, table.name + '.sql')
        logger.info('Writing table definition: {0}'.format(sql_path))
        with open(sql_path, 'w') as sql_file:
            sql_file.write(table.create_table)

    # Build options for the clickhouse-client. 
    opts = ''
    if host:
        opts += " --host={0}".format(host)
    if database:
        opts += " --database={0}".format(database)

    # Define dump scripts for each partition of each table.
    dump_operations = []
    for table in tables:
        logger.info("Generating table dump command: {0}".format(table.name))
        table_path = os.path.join(data_path, table.name)
        os.makedirs(table_path, exist_ok=overwrite)
        partitions = ch.fetch_partitions(table)
        for partition_key, select in partitions:
            if partition_key is None:
                tag = "all"
            else:
                # URL-encode and remove single quotes and forward slashes.
                tag = urllib.parse.quote(str(partition_key))
                tag = tag.replace("/", "_")
            file_path = os.path.join(table_path, "data-{0}.csv".format(tag))
            client_cmd = (
                "clickhouse-client{0} --query=\"{1}\"".format(opts, select))
            if compress:
                client_cmd += "| gzip"
                file_path += ".gz"
            dump_command = client_cmd + " > " + file_path
            dump_operations.append((table.name, partition_key, dump_command))

    # Execute the load commands.
    pool = ProcessPool(
        size=parallel, 
        dry_run=dry_run, 
        progress_reporter=progress_reporter)
    for name, key, cmd in dump_operations:
        _progress_and_info(
            "Dumping data: table={0}, partition={1}".format(name, key),
            progress_reporter)
        pool.exec(cmd)
    pool.drain()
    logger.info(pool.outputs)
    _progress_and_info(
        "Operation summary: succeeded={0}, failed={1}".format(pool.succeeded, pool.failed),
        progress_reporter)

def _progress_and_info(message, progress_reporter):
    if progress_reporter is not None:
        progress_reporter(message)
    logger.info(message)
