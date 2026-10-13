"""STREAMING: every minute, call an API and store the answer in raw.rides."""
import os
import random
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from db import get_conn


def pull_rides():
    # ONE simple call: the API returns 10 fake shopping carts
    # (a dummy API, used only to mimic the effect of a real ride API: see README)
    url = f"https://dummyjson.com/carts?limit=10&skip={random.randint(0, 190)}"
    verify = os.environ.get("VERIFY_SSL", "true") == "true"   # see README if your network blocks HTTPS
    carts = requests.get(url, timeout=10, verify=verify).json()["carts"]

    now = datetime.utcnow()
    conn = get_conn()
    with conn, conn.cursor() as cur:
        for cart in carts:
            # NOTE: DummyJSON has no rides, it only mimics "an API call". We reuse the cart data
            # and map it onto InDrive's world (the mapping is arbitrary, only the shape matters).
            ride_id = f"ride_{now:%Y%m%d%H%M%S}_{cart['id']}"
            pickup = cart["products"][0]["id"] % 8 + 1                  # API product -> one of our 8 zones
            dropoff = (pickup + len(cart["products"]) % 7) % 8 + 1     # always another zone
            driver_id = 999 if cart["id"] % 20 == 0 else cart["userId"] % 8 + 1   # API user -> one of our 8 drivers, 5% unknown (999)
            distance_km = round(1 + len(cart["products"]) * 1.8, 1)
            fare_amount = round(10 + cart["total"] % 120, 2)
            cur.execute(
                "INSERT INTO raw.rides VALUES (%s, %s, %s, %s, %s, %s, %s)",
                [ride_id, driver_id, pickup, dropoff, now, distance_km, fare_amount],
            )
    conn.close()
    print(f"{len(carts)} rides saved in raw.rides")


with DAG(
    dag_id="1_streaming",
    schedule="* * * * *",          # every minute
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["indrive"],
) as dag:
    PythonOperator(task_id="pull_rides", python_callable=pull_rides)
