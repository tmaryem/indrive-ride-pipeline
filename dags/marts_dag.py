"""MARTS: staging -> mart (business tables). Started by the staging DAG, no schedule of its own."""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from db import run_sql_file


def sql_task(name):
    # TODO 1: return a PythonOperator (task_id=name) that runs sql/mart/<name>.sql with run_sql_file
    return PythonOperator(task_id=name, python_callable=run_sql_file, op_args=[f"mart/{name}.sql"])


with DAG(
    dag_id="4_marts",
    schedule=None,                 # started by 3_staging
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["indrive"],
) as dag:
    # TODO 2: create the 3 tasks "rides", "revenue_by_zone", "driver_leaderboard"
    rides = sql_task("rides")
    revenue_by_zone = sql_task("revenue_by_zone")
    driver_leaderboard = sql_task("driver_leaderboard")
    revenue_by_vehicle_type = sql_task("revenue_by_vehicle_type")
    # TODO 3: "rides" first, then the two others in parallel (>> [a, b])
    rides >> [revenue_by_zone, driver_leaderboard, revenue_by_vehicle_type] 
