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
import os
import re

from clickhouse_driver import Client
import yaml

from altinity_datasets.proc_pool import ProcessPool

# Base directory of the installation.
BASE = os.path.join(os.path.dirname(__file__), '..')

# A list of known repo locations.
REPOS = [
    {
        'name': 'built-ins',
        'description': 'Built-in dataset repository',
        'path': os.path.realpath(os.path.join(BASE, 'built-ins'))
    },
]


def _sql(conn, sql, verbose=False, dry_run=False):
    """Execute a SQL command"""
    if verbose:
        print("DEBUG: {0}".format(sql))
    if not dry_run:
        conn.execute(sql)


def repos():
    """List known repos"""
    return REPOS


def search(name, repo_path=None):
    """Search for dataset(s)
    :param name: (str): If specified show only datasets that match this name
    :param repo_path: (str): A path to the repo.  If specified search this
                             repo path only.  Otherwise search default path(s).
    """
    datasets = []
    if repo_path is None:
        search_list = [repo['path'] for repo in REPOS]
    else:
        search_list = [repo_path]

    for repo_path in search_list:
        dir = os.path.join(BASE, repo_path)
        children = [os.path.join(dir, child) for child in os.listdir(dir)]
        for child in children:
            manifest_yaml = os.path.join(child, 'manifest.yaml')
            # Skip if directory has no manifest or if the name does not match.
            if not os.path.exists(manifest_yaml):
                continue
            if name and name != os.path.basename(child):
                continue
            with open(manifest_yaml, "r") as f:
                manifest = yaml.safe_load(f)
            manifest['repo'] = os.path.basename(repo_path)
            manifest['path'] = child
            manifest['name'] = os.path.basename(child)
            datasets.append(manifest)

    return datasets


def load(name,
         repo_path=None,
         host='localhost',
         database=None,
         parallel=5,
         clean=False,
         verbose=False,
         dry_run=False):
    """Load a sample data set
    :param name: (str): Name of dataset
    :param repo_path: (str): Repo name or None to search all repos
    :param host: (str): ClickHouse server host 
    :param database: (str): Database (defaults to dataset name)
    :param parallel: (int): Number of processes to run in parallen when loading
    :param clean: (boolean): If True wipe out existing data
    :param dry_run: (boolean): If True print commands instead of executing them
    """
    # Look up the dataset.
    datasets = search(name, repo_path=repo_path)
    if len(datasets) == 0:
        raise ("Dataset not found: {0}".format(name))
    elif len(datasets) > 1:
        raise (
            "Dataset name is ambiguous, must specify repo path: {0}"
            .format(name))
    else:
        dataset = datasets[0]

    # Use name as the database unless overridden by caller. 
    database = name if database is None else database
    print("Loading to host: {0} database: {1}".format(host, database))

    # Clear database if requested. This connection cannot use the database
    # as it might not exist yet. 
    client_0 = Client(host)
    if clean:
        print("Dropping database if it exists: {0}".format(database))
        _sql(
            client_0,
            "DROP DATABASE IF EXISTS {0}".format(database),
            dry_run=dry_run)

    # Create database.
    print("Creating database if it does not exist: {0}".format(database))
    _sql(client_0, "CREATE DATABASE IF NOT EXISTS {0}".format(database),
         dry_run)

    # We can now safely reference the database.
    client = Client(host, database=database)

    # Load table definitions in sequence.
    ddl_path = os.path.join(dataset['path'], "ddl")
    for sql_file in glob.glob(ddl_path + "/*"):
        print("Executing SQL script: {0}".format(sql_file))
        with open(sql_file, 'r') as f:
            script = f.read()
        _sql(client, script, dry_run=dry_run)

    # Build options for the clickhouse-client. 
    opts = ''
    if host:
        opts += " --host={0}".format(host)
    if database:
        opts += " --database={0}".format(database)

    # Define load scripts for each CSV load file.
    data_path = os.path.join(dataset['path'], "data")
    load_commands = []
    for table_dir in glob.glob(data_path + "/*"):
        print("Processing table data: {0}".format(table_dir))
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
            load_commands.append(load_command)

    # Execute the load commands.
    pool = ProcessPool(size=parallel, dry_run=dry_run)
    for cmd in load_commands:
        pool.exec(cmd)
    pool.drain()
    print(pool.outputs)
