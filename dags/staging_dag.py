"""STAGING: wait for streaming + batch, then raw -> staging (clean, cast types). Then starts the marts DAG."""
from datetime import datetime

from airflow import DAG
from airflow.models import DagRun
from airflow.models.baseoperator import cross_downstream
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.python import PythonSensor

from db import run_sql_file


def has_succeeded(dag_id):
    """True once the given DAG has at least one successful run."""
    return len(DagRun.find(dag_id=dag_id, state="success")) > 0


with DAG(
    dag_id="3_staging",
    # TODO: schedule = every 2 minutes (cron syntax)
    schedule="*/2 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["indrive"],
) as dag:
    # GIVEN: 2 sensors. They wait until "1_streaming" and "2_batch" have each succeeded once.
    waits = [
        PythonSensor(
            task_id=f"wait_{name}",
            python_callable=has_succeeded,
            op_args=[dag_id],
            mode="reschedule",
            poke_interval=10,
            timeout=90,
            soft_fail=True,        # nothing to stage yet -> skip this run, try again in 2 min
        )
        for name, dag_id in [("streaming", "1_streaming"), ("batch", "2_batch")]
    ]

    # TODO 1: one PythonOperator per SQL file of sql/staging/ (task_id "stg_<name>",
    #         python_callable=run_sql_file, op_args=["staging/<name>.sql"])
    #         tip: a list comprehension over ["drivers", "vehicles", "zones", "rides"]
    #         keep the list in a variable called staging_tasks
    staging_tasks = [
        PythonOperator(
            task_id=f"stg_{name}",
            python_callable=run_sql_file,
            op_args=[f"staging/{name}.sql"]
        )
        for name in ["drivers", "vehicles", "zones", "rides"]
    ]

    # TODO 2: a TriggerDagRunOperator (task_id "start_marts") that starts the DAG "4_marts"
    start_marts = TriggerDagRunOperator(
        task_id="start_marts",
        trigger_dag_id="4_marts",
        wait_for_completion=False,
    )

    # TODO 3: wire it: every wait must finish before every staging task, and the 4 staging
    #         tasks must finish before start_marts.
    #         tip: cross_downstream(waits, staging_tasks) + staging_tasks >> start_marts
    cross_downstream(waits, staging_tasks)
    staging_tasks >> start_marts
   
