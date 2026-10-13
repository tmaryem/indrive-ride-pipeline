-- TODO: build mart.revenue_by_zone from mart.rides
-- columns: pickup_zone, rides (count), revenue (sum of fare_amount, rounded to 2)
-- biggest revenue first
drop TABLE IF EXISTS mart.revenue_by_zone;
CREATE TABLE mart.revenue_by_zone AS
SELECT
    r.pickup_zone,
    COUNT(*) AS rides,
    ROUND(SUM(r.fare_amount), 2) AS revenue
FROM mart.rides r
GROUP BY r.pickup_zone
ORDER BY revenue DESC;
