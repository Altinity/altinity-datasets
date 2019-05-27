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
#
import logging
import platform

import click
import pkg_resources

import altinity_datasets.api as api

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS, invoke_without_command=True)
@click.pass_context
@click.option(
    '-V',
    '--verbose',
    is_flag=True,
    default=False,
    help='Log debug output')
@click.option(
    '-L',
    '--log-file',
    default="datasets.log",
    help='Set name of log file')
def ad_cli(ctx, verbose, log_file):
    """Altinity Dataset CLI"""
    if ctx.invoked_subcommand is None:
        click.secho(ctx.get_help())
        return

    if verbose:
        logging.basicConfig(filename=log_file, level=logging.DEBUG)
    else:
        logging.basicConfig(filename=log_file, level=logging.INFO)


@ad_cli.command(short_help='Show version')
@click.pass_context
def version(ctx):
    """Show version"""
    try:
        version = pkg_resources.require("altinity-datasets")[0].version
    except:
        version = '0.0.0'
    version_string = 'ad-cli {0}, Python {1}'.format(version,
                                                     platform.python_version())
    print(version_string)

@ad_cli.group(short_help='Manage dataset repositories')
@click.pass_context
def repo(ctx):
    """Operations to manage dataset repositories"""

@repo.command(short_help='List dataset repositories')
@click.pass_context
def list(ctx):
    """List dataset repositories"""
    repos = api.repos()
    _print_dict_vertical(repos, ['name', 'description', 'path'])

@ad_cli.group(short_help="Manage datasets")
@click.pass_context
def dataset(ctx):
    """Operations to dump, load, and search for datasets"""
    pass

@dataset.command(short_help='Search for dataset(s)')
@click.pass_context
@click.argument('name', metavar='<name>', required=False)
@click.option('-r', '--repo-path', help='Use this repo path')
@click.option('-f', '--full', help='Show full description')
def search(ctx, name, repo_path, full):
    datasets = api.dataset_search(name, repo_path=repo_path)
    _print_dict_vertical(datasets, [
        'name', 'title', 'description', 'size', 'sources',
        'notes', 'repo', 'path'
    ])

@dataset.command(short_help='Load a dataset from files to database')
@click.pass_context
@click.argument('name', metavar='<name>', required=True)
@click.option('-r', '--repo-path', default=None, help='Datasets repository')
@click.option('-H', '--host', default='localhost', help='Server host')
@click.option('-d', '--database', help='Database (defaults to dataset name)')
@click.option('-P', '--parallel', default=5, help='Number of threads to run in parallel')
@click.option(
    '-C',
    '--clean',
    is_flag=True,
    default=False,
    help='Clean existing database')
@click.option(
    '-D', '--dry_run', is_flag=True, default=False, help='Print commands only')
def load(ctx, name, repo_path, host, database, parallel, clean, dry_run):
    api.dataset_load(
        name,
        repo_path=repo_path,
        host=host,
        database=database,
        parallel=parallel,
        clean=clean,
        dry_run=dry_run,
        progress_reporter=_print_progress)

@dataset.command(short_help='Dump a live dataset from database to files')
@click.pass_context
@click.argument('name', metavar='<name>', required=True)
@click.option('-r', '--repo-path', default='.', help='Datasets repository')
@click.option('-H', '--host', default='localhost', help='Server host')
@click.option('-d', '--database', help='Database (defaults to dataset name)')
@click.option('-t', '--tables', help='Table selector regex (defaults to all')
@click.option('-P', '--parallel', default=5, help='Number of threads to run in parallel')
@click.option('-o', '--overwrite', is_flag=True, help='Overwrite existing files', default=False)
@click.option('-c', '--compress', is_flag=True, help='Compress data files', default=False)
@click.option(
    '-D', '--dry_run', is_flag=True, default=False, help='Print commands only')
def dump(ctx, name, repo_path, host, database, tables, parallel, overwrite, compress, dry_run):
    api.dataset_dump(
        name,
        repo_path=repo_path,
        host=host,
        database=database,
        table_regex=tables,
        parallel=parallel,
        overwrite=overwrite,
        compress=compress,
        dry_run=dry_run,
        progress_reporter=_print_progress)

def _print_progress(message):
    """Progress reporting function for long-running operations"""
    print(message)

def _print_dict_vertical(dictionaries, columns):
    """Print dictionary contents vertically"""
    max_width = 0
    for col in columns:
        max_width = max(len(col), max_width)
    format_string = "{{0:<{0}}}: {{1}}".format(max_width)
    # Print data.
    for d in dictionaries:
        print("--------------------------------------------------------")
        for col in columns:
            print(format_string.format(col, d.get(col)))


if __name__ == '__main__':
    ad_cli()
