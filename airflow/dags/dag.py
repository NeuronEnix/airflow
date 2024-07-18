# dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime


def print_hello():
    return "Hello, World!"


def choose_branch():
    # Simple condition for branching
    if datetime.now().minute % 2 == 0:
        return "even_minute_task"
    else:
        return "odd_minute_task"


dag = DAG(
    "hello_world",
    description="Simple tutorial DAG with branching",
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

branch_task = BranchPythonOperator(
    task_id="branch_task",
    python_callable=choose_branch,
    dag=dag,
)

even_minute_task = BashOperator(
    task_id="even_minute_task",
    bash_command="echo 'even_minute_task'",
    dag=dag,
)

odd_minute_task = BashOperator(
    task_id="odd_minute_task",
    bash_command="echo 'odd_minute_task'",
    dag=dag,
)

follow_task = EmptyOperator(
    task_id="follow_task",
    trigger_rule="none_failed_or_skipped",
    dag=dag,
)

bash_task_2 = BashOperator(
    task_id="bash_task_2",
    bash_command="echo 'Another bash'",
    dag=dag,
)

# Define the task dependencies
py_task >> bash_task >> branch_task
branch_task >> [even_minute_task, odd_minute_task] >> follow_task >> bash_task_2
