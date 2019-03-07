#!/usr/bin/env bash
# Reformat code and run flake8 over it to make sure it's correct. 
yapf -i altinity_datasets/*.py
flake8 altinity_datasets/*.py
