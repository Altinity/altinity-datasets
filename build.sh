#/bin/bash
# Build a new release. 
rm -r dist
python3 setup.py sdist
