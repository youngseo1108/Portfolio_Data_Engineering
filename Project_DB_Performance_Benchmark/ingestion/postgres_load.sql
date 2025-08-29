TRUNCATE raw.taxi;
COPY raw.taxi
FROM PROGRAM 'cat /work/data/raw/nyc_taxi_sample.csv'
WITH (FORMAT csv, HEADER true);