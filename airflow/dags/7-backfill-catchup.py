from airflow.decorators import dag, task
from datetime import datetime

# when catchup: False -> to backfill run below command
# airflow dags backfill hello_world -s 2024-07-01 -e 2024-07-22

@dag(
    dag_id="7-backfill-catchup",
    description="Backfill catchup",
    schedule_interval="@daily",
    start_date=datetime(2024, 7, 15),
    catchup=True,
)
def hello_etl():
    @task
    def get_name():
        return "Kaushik"

    @task(multiple_outputs=True)
    def get_dict_name():
        return {"first_name": "Kaushik", "last_name": "Bangera"}

    @task
    def get_age():
        return 20

    @task
    def print_hello(first_name, last_name, age):
        return f"Hello: {first_name} {last_name} ({age})"

    first_name = get_name()
    age = get_age()
    print_hello(first_name, "", age)

    dict_name = get_dict_name()
    print_hello(
        first_name=dict_name["first_name"], last_name=dict_name["last_name"], age=age
    )


hello_etl()
