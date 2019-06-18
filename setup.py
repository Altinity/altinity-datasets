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

"""
Altinity Datasets Utility
"""

import sys
from setuptools import setup, find_packages

# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools

with open('README.md', 'r') as readme_file:
    long_description = readme_file.read()

setup(
    name="altinity_datasets",
    version="0.1.1",
    description="Altinity Datasets for ClickHouse",
    long_description=long_description,
    long_description_content_type='text/markdown',
    license="Apache 2.0",
    author="R Hodges",
    author_email="info@altinity.com", 
    url='https://github.com/Altinity/altinity-datasets',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
    ],
    install_requires=[
        'click>=6.7',
        'clickhouse-driver>=0.0.18',
        'PyYAML>=3.13'
    ],
    packages=find_packages(),
    include_package_data=True,
    entry_points = {
        'console_scripts': ['ad-cli=altinity_datasets.ad_cli:ad_cli']
    }
)
