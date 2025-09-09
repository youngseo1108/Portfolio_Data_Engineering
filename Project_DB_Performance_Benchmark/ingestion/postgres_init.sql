CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

DROP TABLE IF EXISTS raw.taxi;
CREATE TABLE raw.taxi(
  vendor_id VARCHAR(10),
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count INT,
  trip_distance NUMERIC,
  ratecode_id INT,
  store_and_fwd_flag VARCHAR(1),
  pu_location_id INT,
  do_location_id INT,
  payment_type INT,
  fare_amount NUMERIC,
  extra NUMERIC,
  mta_tax NUMERIC,
  tip_amount NUMERIC,
  tolls_amount NUMERIC,
  improvement_surcharge NUMERIC,
  total_amount NUMERIC
);

-- simple analytics view
CREATE OR REPLACE VIEW analytics.trips_by_hour AS
SELECT date_trunc('hour', tpep_pickup_datetime) AS hr,
       COUNT(*) AS trips,
       AVG(total_amount) AS avg_fare
FROM raw.taxi
GROUP BY hr;

-- create index for query performance later (baseline vs tuned)
CREATE INDEX IF NOT EXISTS idx_taxi_pickup ON raw.taxi (tpep_pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_taxi_pu ON raw.taxi (pu_location_id);