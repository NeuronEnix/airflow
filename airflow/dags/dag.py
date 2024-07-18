# dag.py

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime


def print_hello():
    return "Hello, World!"


dag = DAG(
    "hello_world",
    description="Simple tutorial DAG",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 6, 1),
    catchup=True,
)

py_task = PythonOperator(
    task_id="py_task", python_callable=print_hello, dag=dag
)

bash_task = BashOperator(
    task_id="bash_task",
    bash_command="echo 'Hello, Bash!'",
    dag=dag,
)

py_task >> bash_task
