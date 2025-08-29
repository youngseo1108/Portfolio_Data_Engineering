-- Q1 tuned: pre-aggregate (same as baseline; serves as control)
SELECT hr, COUNT(*) trips 
FROM analytics.trips_by_hour
GROUP BY hr
ORDER BY hr;

-- Q2 tuned: add multi-column index suggestion in README; query same
SELECT COUNT(*) 
FROM raw.taxi
WHERE pu_location_id IN (1, 2, 3)
AND tpep_pickup_datetime >= '2019-01-15'
AND tpep_pickup_datetime <  '2019-02-01';

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