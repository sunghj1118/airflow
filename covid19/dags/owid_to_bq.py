from owid_covid_dag.extract import Extract
from owid_covid_dag.transform import Transform
from owid_covid_dag.load import Load
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
import datetime as dt

default_args = {
    'retries': 2,
    'retry_delay': dt.timedelta(minutes=1),
    'email_on_retry': False,
    'email_on_failure': False,
}

dag = DAG(
    'owid_to_bq',
    default_args=default_args,
    start_date=dt.datetime(2022, 2, 1),
    schedule_interval=dt.timedelta(days=1),
    catchup=True
)


def run_etl(ds=None):
    # for each day, load the previous day
    extract = Extract(dt.datetime.strptime(ds, '%Y-%m-%d'))
    df = extract.execute_extraction()
    transform = Transform(df)
    transformed_df = transform.transform_data()
    load = Load(transformed_df)
    load.load()


t1 = PythonOperator(
    task_id="run_etl",
    python_callable=run_etl,
    dag=dag
)

t2 = BigQueryCheckOperator(
    task_id="check_bigquery",
    sql="""
    SELECT COUNT(*) 
    FROM DATASET.TABLE t
    where t.date = parse_timestamp('%Y-%m-%d', '{{ds}}')
    """,
    use_legacy_sql=False,
    dag=dag,
    location="Insert your project's location here"
)

t1 >> t2
