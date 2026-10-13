# Lesson 3 · Exercise — InDrive Ride Pipeline (Light version)

**Goal:** build a small data pipeline with Airflow and Postgres, the same recipe as the BIM lab.
Two sources go in, three layers come out: **raw → staging → mart**.

Time: ~60 min. Everything runs locally in Docker.

InDrive is a ride-hailing app in Casablanca. Its data team receives:

- **Batch:** 3 CSV files maintained by the back office (drivers, vehicles, zones).
- **Streaming:** a flow of ride events coming from the app (here, a fake one from a web API).

Your job: clean the data and build the tables the analysts need.

```
 API (dummy)  ──► [1_streaming] ──┐
                                  ├──► RAW ──► [3_staging] ──► STAGING ──► [4_marts] ──► MART
 3 CSV files  ──► [2_batch] ──────┘
```

| Layer | Schema | What it contains |
|---|---|---|
| Raw | `raw` | data exactly as received |
| Staging | `staging` | cleaned data (trimmed text, no duplicates, no bad rows) |
| Mart | `mart` | business tables, ready for analysts |

## What is given vs what you build

| Given (do not change) | You build |
|---|---|
| `docker-compose.yml` | `sql/staging/*.sql` (4 files) |
| `postgres/init.sql` (schemas + 4 raw tables) | `sql/mart/*.sql` (3 files) |
| `data/*.csv` | `dags/staging_dag.py` |
| `dags/db.py`, `dags/streaming_dag.py`, `dags/batch_dag.py` | `dags/marts_dag.py` |
|  | `ANSWERS.md` (the questions at the end: the file is given, you fill it in) |

Each file you build already exists with `TODO` comments inside. Open `dags/batch_dag.py` and `sql/` of the BIM lab as your reference: same shape, other business.

---

## 0. Before you start

You need Docker Desktop running and an internet connection (the API is on the web).
Ports **8083** (Airflow) and **8086** (Adminer) must be free. Stop the BIM lab first (`docker compose down`).

```bash
cd indrive_ride_pipeline_light_STUDENT
docker compose version
```

---

## 1. Look at the sources

```bash
head -n 3 data/drivers.csv data/vehicles.csv data/zones.csv
```

Open `dags/streaming_dag.py`. Every minute it calls an API that returns 10 fake shopping carts. Each cart becomes one ride, saved as one row in `raw.rides`. We use the real cart data and map it onto InDrive:

| From the cart | Becomes | Rule |
|---|---|---|
| `userId` | `driver_id` (1-8) | `userId % 8 + 1` |
| first product `id` | `pickup_zone_id` (1-8) | `id % 8 + 1` |
| number of products | `dropoff_zone_id` (always another zone), `distance_km` | `distance_km = 1 + products * 1.8` |
| `total` | `fare_amount` | `10 + total % 120` |

> **Note: the API is a dummy.** DummyJSON (`/carts`) has nothing to do with ride-hailing, so the mapping above makes no business sense. It is used **only to mimic the effect of a real API** (an HTTP call every minute that returns fresh JSON). No free public ride-hailing API exists, and the real ones need a partner account. It is simply what we can use right now. What matters in this exercise is the pipeline, not the data.

On purpose, **5% of the rides carry `driver_id = 999`**, a driver that does not exist in `drivers.csv`. Your pipeline must deal with them.

Open `postgres/init.sql` and look at the 4 raw tables.

> Answer Q1 and Q2 in `ANSWERS.md` (the file is in this folder: fill it in as you go).

---

## 2. Start everything

```bash
docker compose up -d
```

Wait ~1 minute, then get the Airflow password:

```bash
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

Open **http://localhost:8083**, login `admin` + that password.
You should see `1_streaming` and `2_batch` running. `3_staging` and `4_marts` are empty for now: that is your job.

Open **http://localhost:8086** (Adminer): System `PostgreSQL`, Server `postgres`, Username `indrive`, Password `indrive`, Database `indrive`.
Use the **schema** dropdown on the left to switch between `raw`, `staging` and `mart`.

Check that data arrives:

```sql
SELECT count(*) FROM raw.rides;      -- grows every minute
SELECT * FROM raw.drivers;
SELECT * FROM raw.rides WHERE driver_id = 999;
```

> If `1_streaming` fails with `CERTIFICATE_VERIFY_FAILED` (a network that inspects HTTPS): in `docker-compose.yml` change `VERIFY_SSL: "true"` to `"false"`, then run `docker compose up -d` again.

---

## 3. Build the staging layer

Complete the 4 files in `sql/staging/`. Each one is a **full rebuild**: `DROP TABLE IF EXISTS` + `CREATE TABLE AS SELECT`.

| File | Source | Rules |
|---|---|---|
| `drivers.sql` | `raw.drivers` | keep `driver_id, driver_name, city, rating`. Trim the text columns. Do **not** carry `phone` (personal data the analysts do not need). |
| `vehicles.sql` | `raw.vehicles` | keep `vehicle_id, driver_id, make, model, vehicle_type`. Trim the text columns. |
| `zones.sql` | `raw.zones` | keep `zone_id, zone_name, city`. Trim the text columns. |
| `rides.sql` | `raw.rides` | remove duplicates, drop rides with `fare_amount <= 0`, drop rides whose `driver_id` is not in `raw.drivers`. |

Then write `dags/staging_dag.py`:

- two sensors are given (`wait_streaming`, `wait_batch`): they make staging wait until `1_streaming` and `2_batch` have each succeeded once, so you never stage empty tables
- one task per SQL file (4 tasks, in parallel), all after the 2 sensors
- schedule: every 2 minutes
- when all 4 are done, start the DAG `4_marts` with a `TriggerDagRunOperator`

Copy the pattern from the BIM lab: it is almost the same file.

---

## 4. Build the mart layer

Complete the 3 files in `sql/mart/`.

| File | Content |
|---|---|
| `rides.sql` | **one row per ride**, with `ride_id, requested_at, driver_id, driver_name, rating, pickup_zone, dropoff_zone, distance_km, fare_amount`. Names, not ids. |
| `revenue_by_zone.sql` | per pickup zone: number of rides and total revenue, biggest first. Built from `mart.rides`. |
| `driver_leaderboard.sql` | per driver: `driver_name, vehicle_type, rating`, number of rides and total revenue, biggest first. Built from `mart.rides` (+ `staging.vehicles` for the vehicle type). |

Then write `dags/marts_dag.py`:

- no schedule (`schedule=None`), it is started by `3_staging`
- `rides` first, then the two other marts in parallel

> Hint for `mart.rides`: a ride has **two** zones (pickup and dropoff). You will need `staging.zones` twice in the same query, with two different aliases.

---

## 5. Check your work

Wait for `3_staging` (max 2 min), then in Adminer:

```sql
SELECT count(*) FROM raw.rides;
SELECT count(*) FROM staging.rides;
SELECT count(*) FROM mart.rides;
SELECT * FROM mart.revenue_by_zone;
SELECT * FROM mart.driver_leaderboard;
```

All 4 DAGs must be green (right after startup, the first `3_staging` run may be pink = skipped because streaming or batch has not succeeded yet: the next run goes through), and `mart.rides` must contain **no** ride with `driver_id = 999`.

Re-run `3_staging` by hand (▶ button): the counts must not double. Why?

---

## 6. Questions (write your answers in `ANSWERS.md`)

| # | Question |
|---|---|
| Q1 | Which sources are **batch** and which is **streaming**? Why does the data team handle them differently? Which one is the **fact** and which are the **dimensions**? |
| Q2 | Is this pipeline **ETL** or **ELT**? Justify in 1-2 sentences, using the 3 layers. |
| Q3 | Write the grain of `mart.rides` in one sentence: "one row = ...". |
| Q4 | `staging.zones` is joined twice in `mart.rides`. What do we call this pattern? Why not make two zone tables? |
| Q5 | How many rides are in `raw.rides`, in `staging.rides`? Where did the others go? Why is filtering in staging better than letting the `JOIN` in the mart drop them silently? |
| Q6 | You re-run `3_staging` three times. Why is there no duplicate? What would happen if the SQL used `INSERT INTO` instead of `DROP` + `CREATE TABLE AS`? |
| Q7 | Is `fare_amount` additive, semi-additive or non-additive? And `rating`? What would be wrong with `SUM(rating)`? |
| Q8 | The back office changes a driver's rating in `drivers.csv` and `2_batch` reloads it. What happens to the **old** rides of this driver in `mart.rides`? Is it correct for InDrive? Which technique would keep the rating the driver had *at the time of the ride*? |

---

## 7. Bonus

1. **New mart.** Create `mart.revenue_by_vehicle_type` (economy / comfort / xl) and add it to `4_marts`.
2. **Break it.** In `sql/staging/zones.sql` replace `raw.zones` with `raw.zonez`. Watch `stg_zones` turn red, open **Logs**, see why `4_marts` did not start. Fix it.

---

## Deliverable

- Your folder with the 7 completed files.
- `ANSWERS.md` with Q1-Q8.
- A screenshot of Airflow with the 4 DAGs green.

---

## Cleanup

```bash
docker compose down        # stop (data is kept in the pgdata and airflow_home volumes)
docker compose down -v     # stop and delete the volumes (full reset)
```

Postgres data and Airflow history are stored in Docker volumes, so they survive restarts. `postgres/init.sql` only runs on an empty volume: after editing it, do a full reset with `down -v`.

## Troubleshooting

| Problem | Fix |
|---|---|
| Airflow page not loading | wait 1 min, then `docker compose logs airflow` |
| DAGs not visible yet | wait 30 s and refresh, the scheduler scans the folder regularly |
| `3_staging` run is skipped (pink) | `1_streaming` or `2_batch` has not succeeded yet: trigger it, the next staging run goes through |
| `3_staging` shows "Import Error" | there is a Python error in `dags/staging_dag.py`: click the red banner to read it |
| `CERTIFICATE_VERIFY_FAILED` | set `VERIFY_SSL` to `"false"` (step 2) |
| Port already in use | change the left number in `"8083:8080"` / `"8086:8080"` in `docker-compose.yml` |
| Adminer cannot connect | Server must be `postgres` (not `localhost`) |
| `staging.zones` does not exist | `3_staging` has not run yet: wait 2 min or trigger it |
