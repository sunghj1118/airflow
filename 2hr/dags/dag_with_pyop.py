from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'hyunjoon',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key= 'first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key= 'last_name')
    age = ti.xcom_pull(task_ids='get_age', key= 'age')
    print(f"Hello World! My name is {first_name} {last_name}, "
          f" and I am {age} years old.")
    
def get_name(ti):
    ti.xcom_push(key='first_name', value='Hyunjoon')
    ti.xcom_push(key='last_name', value='Sung')

def get_age(ti):
    ti.xcom_push(key='age', value=23)

with DAG(
    default_args=default_args,
    dag_id='dag_with_pyop_v6',
    description='This is our first dag that we write with python operator',
    start_date=datetime(2022, 9, 29, 2),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        # op_kwargs={
        #     'age': 23
        # }
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )
     
    task3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )

    [task2, task3] >> task1

