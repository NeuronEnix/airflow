from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG(
    "5-xcom-push-pull",
    description="Bash and python operator",
    schedule_interval="0 3 * * *",  # 3 times a day
    start_date=datetime(2024, 7, 22),
    # catchup=True,
)


# def print_hello(user_arg, task_instance): # last arg is task instance i think
def print_hello(task_instance):
    first_name = task_instance.xcom_pull(task_ids="get_name", key="first_name")
    last_name = task_instance.xcom_pull(task_ids="get_name", key="last_name")
    return f"Hello from python {first_name} {last_name}"


def get_name(ti):
    ti.xcom_push(key="first_name", value="Kaushik")
    ti.xcom_push(key="last_name", value="Bangera")


get_name_task = PythonOperator(task_id="get_name", python_callable=get_name, dag=dag)
print_hello_task = PythonOperator(
    task_id="print_hello", python_callable=print_hello, dag=dag
)

# Define the task dependencies
get_name_task >> print_hello_task
