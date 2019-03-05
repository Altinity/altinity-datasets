# Copyright (c) 2019 Altinity LTLD
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
import click
import pkg_resources
import platform

import altinity_datasets.api as api

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
@click.group(context_settings=CONTEXT_SETTINGS, invoke_without_command=True)
@click.pass_context
@click.option(
    '-V', '--verbose', is_flag=True, default=False, help='Print verbose output')
def ad_cli(ctx, verbose):
    """Altinity Dataset CLI"""
    if ctx.invoked_subcommand is None:
        click.secho(ctx.get_help())
        return

@ad_cli.command(short_help='Show version')
@click.pass_context
def version(ctx):
    """Show version"""
    try:
        version = pkg_resources.require("ds-cli")[0].version
    except:
        version = '0.0'
    version_string = 'ds-cli {0}, Python {1}'.format(version, platform.python_version())
    print(version_string)

@ad_cli.command(short_help='List dataset repositories')
@click.pass_context
def repos(ctx):
    """Show available dataset repositories"""
    repos = api.repos()
    _print_dict_vertical(repos, ['name', 'description'])

@ad_cli.command(short_help='Describe dataset(s)')
@click.pass_context
@click.argument('name', metavar='<name>', required=False)
@click.option(
    '-r', '--repo', default='built-ins', help='Datasets repository')
def describe(ctx, name, repo):
    datasets = api.describe(name, repo=repo)
    _print_dict_vertical(datasets, 
                         ['repo', 'name', 'title', 'description', 'size', 'sources'])

@ad_cli.command(short_help='Load dataset')
@click.pass_context
@click.argument('name', metavar='<name>', required=True)
@click.option(
    '-r', '--repo', default='built-ins', help='Datasets repository')
@click.option(
    '-h', '--host', default='localhost', help='Server host')
@click.option(
    '-p', '--parallel', default=5, help='Number of threads to run in parallel')
@click.option(
    '-C', '--clean', is_flag=True, default=False, help='Clean existing database')
@click.option(
    '-D', '--dry_run', is_flag=True, default=False, help='Print commands only')
def load(ctx, name, repo, host, parallel, clean, dry_run):
    print("{0} {1} {2} {3} {4}".format(name, host, parallel, clean, dry_run))
    api.load(name, repo=repo, host=host, parallel=parallel, clean=clean, dry_run=dry_run)

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
