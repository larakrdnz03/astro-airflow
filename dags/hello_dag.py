from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print("Merhaba Astro!")

with DAG(
    dag_id="hello_test_dag",
    start_date=datetime(2025, 7, 1),
    schedule="@hourly",
    catchup=False
) as dag:
    task = PythonOperator(
        task_id="hello_task",
        python_callable=say_hello
    )
<<<<<<< HEAD
=======


>>>>>>> f2cd145 (Add hello DAG)
