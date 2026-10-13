DROP TABLE IF EXISTS mart.revenue_by_vehicle_type;
CREATE TABLE mart.revenue_by_vehicle_type AS
SELECT
    v.vehicle_type,
    COUNT(*) AS rides,
    ROUND(SUM(r.fare_amount), 2) AS revenue
FROM mart.rides r
JOIN staging.vehicles v ON v.driver_id = r.driver_id
GROUP BY v.vehicle_type
ORDER BY revenue DESC;