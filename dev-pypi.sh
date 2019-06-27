#!/usr/bin/env bash
# Create and post release to pypi.org.  User and password for
# PyPI must be set in TWINE_USERNAME and TWINE_PASSWORD respectively.  
twine upload --repository-url https://upload.pypi.org/legacy/ dist/*
