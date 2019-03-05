# Copyright (c) 2019 Altinity

"""
Altinity Datasets Library
"""

import sys
from setuptools import setup, find_packages

# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools

setup(
    name="altinity_datasets",
    version="0.0.1",
    description="Altinity Datasets for ClickHouse",
    install_requires=[
        'click>=6.7',
        'clickhouse-driver>=0.0.18',
        'PyYAML>=3.13'
    ],
    packages=find_packages(),
    include_package_data=True,
    long_description="""\
        Library to load sample datasets for ClickHouse
    """,
    entry_points = {
        'console_scripts': ['ad-cli=altinity_datasets.ad_cli:ad_cli']
    }
)
