-- TODO: rebuild staging.zones from raw.zones
-- keep zone_id, zone_name, city. Trim the text columns.
Drop TABLE IF EXISTS staging.zones;
CREATE TABLE staging.zones AS
SELECT
    zone_id,
    TRIM(zone_name) AS zone_name,
    TRIM(city) AS city
FROM raw.zones;