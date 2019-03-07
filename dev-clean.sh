#!/usr/bin/env bash
# Clean up dev directories and files. 
find . -name __pycache__ -exec rm -r {} \;
rm -r dist
rm -r altinity_datasets.egg-info
