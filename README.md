In this module of the Data Engineering Zoomcamp, I focused on designing and implementing an ETL pipeline for NYC taxi data using Bruin, leveraging BigQuery as our destination database. This project demonstrates how to ingest raw data, stage it for consistency, and generate reports all while maintaining clear lineage and orchestration.

Architecture

Press enter or click to view image in full size

The pipeline follows a three-layered architecture:

Ingestion Layer: Extracts raw taxi trip data and payment lookup data.
Staging Layer: Cleans, deduplicates, and joins the raw data.
Reports Layer: Aggregates trips, calculates revenue metrics, and prepares the data for analytics.
Bruin automatically handles asset dependencies, ensuring that each layer runs only after its upstream assets are complete. This also allows visualizing the data lineage within the Bruin UI.

Become a Medium member
Project structure:

zoomcamp/
├── .bruin.yml
├── README.md
└── pipeline/
    ├── pipeline.yml
    └── assets/
        ├── ingestion/
        │   ├── trips.py
        │   ├── requirements.txt
        │   ├── payment_lookup.asset.yml
        │   └── payment_lookup.csv
        ├── staging/
        │   └── trips.sql
        └── reports/
            └── trips_report.sql
.bruin.yml – Environment Config

default_environment: default
environments:
    default:
        connections:
            google_cloud_platform:
                - location: US
                  name: bigquery-default
                  project_id: solar-router-483810-s0
                  service_account_file: /mnt/c/Users/steph/Downloads/solar-router-483810-s0-219c4dace8e9.json
pipeline.yml – Pipeline Definition

name: nyc_taxi
schedule: daily
start_date: "2022-01-01"
default_connections:
  bigquery: bigquery-default
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow"]
taxi_types can be overridden at runtime with --var to control which taxi types are ingested.
start_date determines the starting point for incremental or full-refresh runs.
Ingestion Layer -Trips Python Asset (trips.py)
The ingestion script fetches NYC taxi data from public Parquet files hosted online:

"""@bruin
name: ingestion.trips
type: python
image: python:3.11

connection: bigquery-default

destination:
  dataset: staging       
  table: trips           

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
@bruin"""

import os
import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ.get("BRUIN_VARS", "{}")).get("taxi_types", ["yellow"])

    parquet_columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

    start_dt = datetime.fromisoformat(start_date)
    end_dt = datetime.fromisoformat(end_date)

    final_df = pd.DataFrame(columns=["pickup_datetime", "dropoff_datetime"])

    current_dt = start_dt
    while current_dt < end_dt:
        year = current_dt.year
        month = current_dt.month

        for taxi_type in taxi_types:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            try:
                df = pd.read_parquet(url, columns=parquet_columns)

                # Rename columns to match the asset schema
                df = df.rename(columns={
                    "tpep_pickup_datetime": "pickup_datetime",
                    "tpep_dropoff_datetime": "dropoff_datetime"
                })

                final_df = pd.concat([final_df, df], ignore_index=True)
                print(f"Loaded {len(df)} rows for {taxi_type} {year}-{month:02d}")

            except Exception as e:
                print(f"Warning: failed to load {url}: {e}")

        current_dt += relativedelta(months=1)

    return final_df
Returns a Pandas DataFrame. Bruin handles inserting it into BigQuery.
Uses append strategy to preserve previously ingested rows.
Dynamic BRUIN_START_DATE / BRUIN_END_DATE variables allow incremental runs.
Payment Lookup Asset (payment_lookup.asset.yml)

name: ingestion.payment_lookup
type: bq.seed
connection: bigquery-default  # <--- important
parameters:
  path: payment_lookup.csv
columns:
  - name: payment_type_id
    type: INT64
    description: "Numeric code for payment type"
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: payment_type_name
    type: STRING
    description: "Human-readable payment type"
    checks:
      - name: not_null
Staging Layer — SQL Asset (staging/trips.sql)
/* @bruin
name: staging.trips
type: bq.sql

depends:
  - ingestion.trips

connection: bigquery-default

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    checks:
      - name: not_null
@bruin */

SELECT
    pickup_datetime,
    dropoff_datetime
FROM `solar-router-483810-s0.ingestion.trips`
WHERE pickup_datetime >= TIMESTAMP('{{ start_datetime }}')
  AND pickup_datetime < TIMESTAMP('{{ end_datetime }}')
Reports Layer — SQL Asset (reports/trips_report.sql)
/* @bruin
name: reports.trips_report
type: bq.sql

depends:
  - staging.trips

connection: bigquery-default

materialization:
  type: table
  strategy: time_interval
  incremental_key: trip_date
  time_granularity: date
  create_if_not_exists: true

columns:
  - name: trip_date
    type: date
    primary_key: true
  - name: trip_count
    type: bigint
    checks:
      - name: non_negative
@bruin */

SELECT
    DATE(pickup_datetime) AS trip_date,
    COUNT(*) AS trip_count
FROM `solar-router-483810-s0.staging.trips`
WHERE pickup_datetime >= TIMESTAMP('{{ start_datetime }}')
  AND pickup_datetime < TIMESTAMP('{{ end_datetime }}')
GROUP BY DATE(pickup_datetime)
Running the Pipeline
# Validate the pipeline
bruin validate ./pipeline/pipeline.yml

# Full refresh for all historical data
bruin run ./pipeline/pipeline.yml --full-refresh

# Run a small date range for testing
bruin run ./pipeline/pipeline.yml --start-date 2022-01-01 --end-date 2022-02-01

# Query results
bruin query --connection bq-prod --query "SELECT COUNT(*) FROM ingestion.trips"
This module demonstrates a production-ready ETL pipeline using Bruin and BigQuery:

Ingestion: Pulls raw data from external Parquet and CSV sources.
Staging: Cleans, deduplicates, and joins data for downstream use.
Reporting: Aggregates metrics for analytics.
Materialization Strategies: append, time_interval, and table ensure flexible incremental and full-refresh operations.
Data Lineage: Bruin automatically tracks dependencies across all assets.
Here’s my homework solution: https://github.com/stephandoh/zoomcamp54456

Following along with this amazing free course with the link below:

You can sign up here: https://github.com/DataTalksClub/data-engineering-zoomcamp/
