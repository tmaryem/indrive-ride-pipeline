-- Runs once, the first time the postgres container starts.
-- Three layers = three schemas.
CREATE SCHEMA raw;      -- data as it arrived
CREATE SCHEMA staging;  -- cleaned data
CREATE SCHEMA mart;     -- business tables for analysts

-- RAW tables: where the DAGs drop the data
-- (staging and mart tables are created by the DAGs themselves)
CREATE TABLE raw.drivers (
    driver_id   INT,
    driver_name TEXT,
    phone       TEXT,
    city        TEXT,
    rating      NUMERIC,
    joined_date DATE
);

CREATE TABLE raw.vehicles (
    vehicle_id   INT,
    driver_id    INT,
    make         TEXT,
    model        TEXT,
    plate_number TEXT,
    vehicle_type TEXT
);

CREATE TABLE raw.zones (
    zone_id   INT,
    zone_name TEXT,
    city      TEXT
);

-- one row per ride
CREATE TABLE raw.rides (
    ride_id         TEXT,
    driver_id       INT,
    pickup_zone_id  INT,
    dropoff_zone_id INT,
    requested_at    TIMESTAMP,
    distance_km     NUMERIC,
    fare_amount     NUMERIC
);
