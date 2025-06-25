import snowflake.connector
import os
from airflow import DAG
from airflow.decorators import task
from dotenv import load_dotenv
from datetime import timedelta, datetime


load_dotenv('../env_file.yaml')

default_args = {
    'owner': 'mohit',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    's3-snowflake-ETL',
    default_args=default_args,
    description='ETL job from delta lake to snowflake',
    start_date=datetime(2020, 1, 1),
    schedule=timedelta(days=1),
    tags={'s3_delta_lake', 'snowflake'}
)

@task(task_id='s3_ingestion', dag=dag)
def extract_from_s3():
    pass

@task(task_id='delta_lake_to_snowflake', dag=dag)
def transform():
    pass

@task(task_id='load_to_warehouse', dag=dag)
def load_to_snowflake():
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
    )
    cursor = conn.cursor()
    cursor.close()
    conn.close()

extract_from_s3() >> transform() >> load_to_snowflake()
