from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG(
    "2-python-with-param",
    description="Pass param to python operator",
    schedule_interval="0 3 * * *",  # 3 times a day
    start_date=datetime(2024, 7, 22),
    # catchup=True,
)


def print_hello(name):
    return "Hello from python: " + name


py_task = PythonOperator(
    task_id="py_task", python_callable=print_hello, dag=dag, op_kwargs={"name": "Kaushik"}
)

bash_task = BashOperator(
    task_id="bash_task",
    bash_command="echo 'Hello from bash'",
    dag=dag,
)

# Define the task dependencies
py_task >> bash_task
