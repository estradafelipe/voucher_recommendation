#!/bin/sh

docker-compose up -d
source venv/bin/activate
python3 etl/etl.py
rm -f data.parquet.gzip
export FLASK_APP=./endpoint/api_endpoint.py
export FLASK_RUN_PORT=8000
export FLASK_DEBUG=1
flask run --debugger -h 0.0.0.0