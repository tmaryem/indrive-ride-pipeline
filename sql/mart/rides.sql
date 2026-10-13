-- TODO: build mart.rides = one row per ride, with names instead of ids
-- columns: ride_id, requested_at, driver_id, driver_name, rating,
--          pickup_zone, dropoff_zone, distance_km, fare_amount
-- sources: staging.rides + staging.drivers + staging.zones (TWICE: two aliases)
Drop TABLE IF EXISTS mart.rides;
CREATE TABLE mart.rides AS
SELECT
    r.ride_id,
    r.requested_at,
    r.driver_id,
    d.driver_name,
    d.rating,
    p.zone_name AS pickup_zone,
    o.zone_name AS dropoff_zone,
    r.distance_km,
    r.fare_amount
FROM staging.rides r
JOIN staging.drivers d ON d.driver_id = r.driver_id
JOIN staging.zones p ON p.zone_id = r.pickup_zone_id
JOIN staging.zones o ON o.zone_id = r.dropoff_zone_id;