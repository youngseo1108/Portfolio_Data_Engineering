-- Q1: Hourly trip counts (tuned)
WITH trips AS (
    SELECT 
        date_trunc('hour', tpep_pickup_datetime)::timestamp AS pickup_hour
    FROM raw.taxi
)
SELECT
    pickup_hour,
    COUNT(*) AS trips
FROM trips
GROUP BY pickup_hour
ORDER BY pickup_hour;

-- Q2 tuned: add multi-column index suggestion in README; query same
SELECT COUNT(*)
FROM raw.taxi
WHERE pu_location_id IN (1, 2, 3)
AND tpep_pickup_datetime >= '2025-01-15'
AND tpep_pickup_datetime < '2025-02-01';

-- Q3 tuned: reduce rows then window
WITH subset AS (
  SELECT pu_location_id, total_amount 
  FROM raw.taxi
  WHERE total_amount > 0
  LIMIT 100000
)
SELECT pu_location_id,
       AVG(total_amount) OVER (PARTITION BY pu_location_id) AS avg_fare
FROM subset;