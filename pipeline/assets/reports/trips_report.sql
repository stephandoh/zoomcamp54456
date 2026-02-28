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