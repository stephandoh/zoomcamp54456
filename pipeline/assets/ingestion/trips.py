"""@bruin
name: ingestion.trips
type: python
image: python:3.11

connection: bigquery-default

destination:
  dataset: staging       # explicitly tell Bruin which dataset to write to
  table: trips           # optional, can default to asset name

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