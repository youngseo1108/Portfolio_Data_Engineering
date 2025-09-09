-- Q1: hourly trips
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


-- Q2: selective (σ) location filter + time window
SELECT COUNT(*)
FROM raw.taxi
WHERE pu_location_id IN (1,2,3)
AND tpep_pickup_datetime >= TIMESTAMP '2025-01-15'
AND tpep_pickup_datetime < TIMESTAMP '2025-02-01';


-- Q3: window function
WITH subset AS (
  SELECT pu_location_id, total_amount 
  FROM raw.taxi
  WHERE total_amount > 0
  LIMIT 100000
)
SELECT pu_location_id,
       AVG(total_amount) OVER (PARTITION BY pu_location_id) AS avg_fare
FROM subset;