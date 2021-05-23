#!/bin/sh
# install the requirements in your venv
pip install -r requirements.txt
# run the creation of schema, tables and populate the tables with data
./scripts/shell/jobs.sh