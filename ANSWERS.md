# ANSWERS — InDrive Ride Pipeline (Light version)

Name: 

Short answers (1-3 sentences). Write them as you go.

## Q1 — Which sources are **batch** and which is **streaming**? Why does the data team handle them differently? Which one is the **fact** and which are the **dimensions**?

**Your answer:**
>Batch sources: drivers.csv, vehicules.csv, and zones.csv, because they are loaded once a day by 2_batch.
>Streaming source: the ride events from the API, because they're pulled every minute by 1_streaming.
>
>They're handled differently because batch sources are rarely change, so a full daily reload is enough. 
>Rides arrive continuously in small pieces, so they are loaded every minute
>
>Fact table is rides, because they are events with measures we can count/sum. 
>Dimensions are drivers, vehicles and zones, because they describle the who and where of each ride


## Q2 — Is this pipeline **ETL** or **ELT**? Justify in 1-2 sentences, using the 3 layers.

**Your answer:**
>The pipeline is ELT, because the data is extracted from the API and the csv files, and loaded into the raw schema; The transformation happens afterwards, inside the postres >using SQL.


## Q3 — Write the grain of `mart.rides` in one sentence: "one row = ..."

Your answer:
>One row =  one ride (one ride_id), requested at a given time (requested_at), driven by one driver, from one pickup zone to one dropoff zone, with its distance and its fare.


## Q4 — `staging.zones` is joined twice in `mart.rides`. What do we call this pattern? Why not make two zone tables?

Your answer:
>Using one dimension to play two different roles is called a role-playing dimension. 
>We don't make two zone tables because it would duplicate the same data and keeping one table gives a single source of truth for zones.


## Q5 — How many rides are in `raw.rides`, in `staging.rides`? Where did the others go? Why is filtering in staging better than letting the `JOIN` in the mart drop them silently?

Your numbers: `raw.rides` = 17730 ; `staging.rides` = 16499

Your answer:
>At the time of the check, raw.rides had 17330 rides and staging.rides had 16499, a gap of 831 rides. The gap comes from the rides whose driver doesn’t exist in raw.drivers.
>The filtering is applied in staging to centralize the rule in one place, so every mart that reads staging.rides gets clean data automatically, instead of handling this rule in each new mart. Also the rules are visibles and explicit in staging and we can count the rejected rows to monitor data quality.

## Q6 — You re-run `3_staging` three times. Why is there no duplicate? What would happen if the SQL used `INSERT INTO` instead of `DROP` + `CREATE TABLE AS`?

Your answer:
>There is no duplicate because each staging SQL file does a full rebuild, the DROP TABLE deletes the old table, and CREATE TABLE ... AS SELECT recreates it from the whole raw table. Running it 1 time or 3 times gives the same result, so the task is idempotent. 
>With INSERT INTO and no DROP, each run would append all the rides from raw again to the rows already in the table. At the end we will have a duplicate data

## Q7 — Is `fare_amount` additive, semi-additive or non-additive? And `rating`? What would be wrong with `SUM(rating)`?

Your answer:
>fare_amount is additive because we can sum it across any dimension, and the result is meaningful. That’s how we compute revenue in the marts.
>rating is non-additive because it’s a score on a scale from 1 to 5, and adding scores doesn’t give any meaningful information.

## Q8 — The back office changes a driver's rating in `drivers.csv` and `2_batch` reloads it. What happens to the **old** rides of this driver in `mart.rides`? Is it correct for InDrive? Which technique would keep the rating the driver had *at the time of the ride*?

Your answer:
>2_batch reloads raw.drivers with TRUNCATE, and the staging and mart tables are fully rebuilt. So the new rating overwrites the old one, and all the driver’s rides, including the old ones, show the new rating in mart.rides. This is SCD Type 1 (overwrite, no history).
>It’s not correct for InDrive: we lose the history, so we can’t analyze performance over time.
>To keep the rating at the time of the ride, we would use SCD Type 2 where each change adds a new row for the driver with valid_from / valid_to columns, and the old row is kept.

## Bonus (optional)

- New mart `mart.revenue_by_vehicle_type`: paste your SQL here.
```sql 
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
```
- Break it (`raw.zonez`): what did you see in the Airflow logs?

>I replaced raw.zones with raw.zonez in *sql/staging/zones.sql*:
>- *stg_zones* failed (red). The log says:  *psycopg2.errors.UndefinedTable: relation "raw.zonez" does not exist*
>- The 3 other staging tasks stayed green.
>- *start_marts* was *upstream_failed* (orange), so `4_marts` did not start because Airflow only runs a task if all the tasks before it succeeded.

>Fix: I put back *raw.zones*, and the next run was green again.