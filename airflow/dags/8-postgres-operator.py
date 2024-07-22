from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

# when catchup: False -> to backfill run below command
# airflow dags backfill hello_world -s 2024-07-01 -e 2024-07-22


@dag(
    dag_id="8-postgres-operator",
    description="Create table and insert data",
    schedule_interval="@daily",
    start_date=datetime(2024, 7, 15),
    catchup=False,
)
def hello_etl():
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres_conn",
        autocommit=True,
        database="test",
        sql="CREATE TABLE IF NOT EXISTS hello (first_name VARCHAR, last_name VARCHAR, age INTEGER);",
    )

    @task(multiple_outputs=True)
    def get_dict_name():
        return {"first_name": "Kaushik", "last_name": "Bangera"}

    @task
    def get_age():
        return 20

    @task
    def print_hello(first_name, last_name, age):
        return f"Hello: {first_name} {last_name} ({age})"

    age = get_age()
    dict_name = get_dict_name()
    print_hello(
        first_name=dict_name["first_name"], last_name=dict_name["last_name"], age=age
    )

    create_table


hello_etl()
