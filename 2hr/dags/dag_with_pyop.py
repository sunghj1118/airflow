from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'hyunjoon',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def greet(name, age):
    print(f"Hello World! My name is {name}, "
          f" and I am {age} years old.")
    
def get_name():
    return 'Minji'

with DAG(
    default_args=default_args,
    dag_id='dag_with_pyop_v3',
    description='This is our first dag that we write with python operator',
    start_date=datetime(2022, 9, 29, 2),
    schedule_interval='@daily'
) as dag:
    # task1 = PythonOperator(
    #     task_id='greet',
    #     python_callable=greet,
    #     op_kwargs={
    #         'name': 'Hyunjoon',
    #         'age': 23
    #     }
    # )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task2

