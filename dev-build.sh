#!/usr/bin/env bash
# Build a new release. 
rm -r dist
tox
python3 setup.py sdist
