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