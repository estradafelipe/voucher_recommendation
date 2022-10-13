# Voucher Recommendation

## Objective
This project attempts to show the interplay between different frameworks in the field of data. The tools that are used in this project are:

- Docker - containerisation and standardisation of processes
- Postgres - Database to store data

This project uses Python, SQL and BASH.

## Data Cleaning

The dataset was filtered and cleaned according to the following criteria:

- Only entries holding the Peru country code were included
- Entries with total_orders of 0 or null were discarded.
- Entries with last_order_ts that took place before first_order_ts were discarded.
- Entries with null voucher_amount were discarded.

## Requirements
In order to run this project, Docker needs to be installed and running. Furthermore, ensure that the ports used in the docker-compose are not blocked by other processes you might already have running.

## Steps to execute

The repository's root directory contains an executable quickstart.sh .
While being in the root directory execute the following to initialise the project:
./quickstart.sh

The executable runs docker-compose, activate de venv, execute etl job and the API


## API enpoints
A REST API is connected to the database and provides access to information via the following endpoints:

- http://localhost:8000/voucher - endpoint that accepts requests and returns a JSON object
- http://localhost:8000/all - endpoint that displays all recommended vouchers for all segments
