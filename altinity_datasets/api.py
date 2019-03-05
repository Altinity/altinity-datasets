#!/usr/bin/env python3

import glob
import os
import pkg_resources
import re
import sys
import time
import yaml

from clickhouse_driver import Client

from altinity_datasets.proc_pool import ProcessPool

# A list of known repo locations. 
REPOS = [
    {'name': 'built-ins', 'description': 'Baked into package'},
]
# Base directory of the installation. 
BASE = os.path.join(os.path.dirname(__file__), '..')

def _sql(conn, sql, verbose=False, dry_run=False):
   """Execute a SQL command"""
   if verbose:
       print("DEBUG: {0}".format(sql))
   if not dry_run:
       conn.execute(sql)

def repos():
    """List known repos"""
    return REPOS

def describe(name, repo=None):
    """Describe one or more data sets
    :param name: (str): If specified show only datasets that match this name
    :param host: (str): If specified search only this repo
    """
    datasets = []
    if repo is None:
        search_list = ['built-ins']
    else:
        search_list = [repo]

    for repo in search_list:
        dir = os.path.join(BASE, repo)
        children = [os.path.join(dir, child) for child in os.listdir(dir)]
        for child in children:
            manifest_yaml = os.path.join(child, 'manifest.yaml')
            if not os.path.exists(manifest_yaml):
                print("Not found " + manifest_yaml)
                continue
            with open(manifest_yaml, "r") as f:
                manifest = yaml.safe_load(f)
            manifest['repo'] = repo
            manifest['name'] = os.path.basename(child)
            datasets.append(manifest)

    return datasets

def load(name, repo=None, host='localhost', parallel=5, clean=False, 
         verbose=False, dry_run=False):
    """Load a sample data set
    :param name: (str): Name of dataset
    :param repo: (str): Repo name or None to search all repos
    :param host: (str): ClickHouse server host name
    :param parallel: (int): Number of processes to run in parallen when loading
    :param clean: (boolean): If True wipe out existing data
    :param dry_run: (boolean): If True print commands instead of executing them
    """
    if not os.path.exists(name) or not os.path.isdir(name):
        raise("Invalid load path: {0}".format(name))

    # Database is the name of the directory. 
    database = os.path.basename(name)
    print("Loading to host: {0} database: {1}".format(host, database))

    # Clear database if requested. 
    client_0 = Client(host)
    if clean:
        print("Dropping database if it exists: {0}".format(database))
        _sql(client_0, 
             "DROP DATABASE IF EXISTS {0}".format(database),
             dry_run=dry_run)

    # Create database. 
    print("Creating database if it does not exist: {0}".format(database))
    _sql(client_0, 
         "CREATE DATABASE IF NOT EXISTS {0}".format(database),
         dry_run)

    # We can now safely reference the database. 
    client = Client(host, database=database)

    # Load table definitions in sequence. 
    ddl_path = os.path.join(name, "ddl")
    for sql_file in glob.glob(ddl_path + "/*"):
        print("Executing SQL script: {0}".format(sql_file))
        with open(sql_file, 'r') as f:
            script = f.read()
        _sql(client, script, dry_run=dry_run)
    
    # Define load scripts for each CSV load file. 
    data_path = os.path.join(name, "data")
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
                print("Unable figure out how to open file: {0}".format(csv_file))
                sys.exit(1)
    
            client_cmd = "clickhouse-client --database={0} --host={1} --query='{2}'".format(database, host, load_sql)
            load_command = cat_cmd + " | " + client_cmd
            load_commands.append(load_command)
    
    # Execute the load commands. 
    pool = ProcessPool(size=parallel, dry_run=dry_run)
    for cmd in load_commands:
        pool.exec(cmd)
    pool.drain()
    print(pool.outputs)
