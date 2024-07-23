from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
from time import sleep

"""
First need to add database connection in Airflow UI
In Airflow UI on the top ribbon go to: Admin -> Connections -> Add (+)
In the form
    Connection Id: postgres_conn; (a name for the connection)
    Connection Type: Postgres
    Host: 127.0.0.1
    Database: test; (table name)
    Login: airflow; (a user name)
    Password: airflow; (a password)
    Port: 5432;
"""

"""
when catchup: False -> to backfill run below command
airflow dags backfill hello_world -s 2024-07-01 -e 2024-07-22
"""


@dag(
    dag_id="8-postgres-operator",
    description="Create table and insert data",
    schedule_interval="@daily",
    start_date=datetime(2024, 7, 15),
    catchup=False,
)
def hello_etl():

    @task
    def create_table():
        # sleep(2)
        SQLExecuteQueryOperator(
            task_id="create_table",
            conn_id="postgres_conn",
            autocommit=True,
            database="test",
            sql="CREATE TABLE IF NOT EXISTS hello (first_name VARCHAR, last_name VARCHAR, age INTEGER);",
        ).execute({})

    @task
    def insert_data(first_name, last_name, age):

        sql = f"INSERT INTO hello VALUES ('{first_name}', '{last_name}', {age});"
        SQLExecuteQueryOperator(
            task_id="insert_table",
            conn_id="postgres_conn",
            autocommit=True,
            database="test",
            sql=sql,
        ).execute({})

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

    create_table() >> insert_data(dict_name["first_name"], dict_name["last_name"], age)


hello_etl()
