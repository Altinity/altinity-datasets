#!/usr/bin/env bash
# Build a new release. 
rm -r dist
set -e
tox
python3 setup.py sdist 
