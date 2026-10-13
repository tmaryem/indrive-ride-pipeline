-- TODO: rebuild staging.drivers from raw.drivers
-- keep driver_id, driver_name, city, rating (no phone). Trim the text columns.
-- Pattern: DROP TABLE IF EXISTS ...; CREATE TABLE ... AS SELECT ...;
DROP TABLE IF EXISTS staging.drivers;
CREATE TABLE staging.drivers AS
SELECT
    driver_id,
    TRIM(driver_name) AS driver_name,
    TRIM(city) AS city,
    rating
FROM raw.drivers;
