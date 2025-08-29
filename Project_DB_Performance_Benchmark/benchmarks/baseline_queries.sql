-- Q1: hourly trips (no index benefit beyond time)
SELECT date_trunc('hour', tpep_pickup_datetime) AS hr,
       COUNT(*) AS trips
FROM raw.taxi
GROUP BY hr
ORDER BY hr;


-- Q2: selective location filter + time window
SELECT COUNT(*)
FROM raw.taxi
WHERE pu_location_id IN (1, 2, 3)
AND tpep_pickup_datetime BETWEEN '2019-01-15' AND '2019-01-31';

-- Q3: window function
SELECT pu_location_id,
       AVG(total_amount) OVER (PARTITION BY pu_location_id) AS avg_fare
FROM raw.taxi
LIMIT 100000;