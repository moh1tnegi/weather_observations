import os
from airflow import DAG
from airflow.decorators import task
from dotenv import load_dotenv
from datetime import timedelta, datetime
from pyspark.sql.functions import col, month, year
import sqlalchemy

from spark_driver import get_spark_session

load_dotenv('../env_file.yaml')

spark = get_spark_session()

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
    df = spark.read.format('delta').load(f's3a://{os.getenv("S3_BUCKET")}/{os.getenv("S3_PATH")}')
    df = df.withColumn('obs_year', year('obs_time')).withColumn('obs_month', month('obs_time'))
    df.cache()
    df.createOrReplaceTempView('weather_temp')
    print('Dataframe schema:')
    df.printSchema()
    return df

@task(task_id='delta_lake_to_snowflake', dag=dag)
def transform(df):
    clean_df = df.dropDuplicates().filter((col('temperature') > -100) & (col('temperature') < 60))
    city_df = clean_df.select(col('stationName'), col('coordinates'))
    return city_df

    # clean_df = df.dropna(subset=['station_id', 'obs_time', 'temperature']).dropDuplicates()
    #
    # agg_df = clean_df.groupby('station_id', 'obs_time', 'temperature') \
    #             .avg('temperature', 'humidity', 'wind_speed')
    # agg_df = agg_df.withColumnRenamed('avg(temperature)', 'avg_temp') \
    #             .withColumnRenamed('avg(humidity)', 'avg_humidity') \
    #             .withColumnRenamed('avg(wind_speed)', 'avg_wind_speed')
    # return agg_df

@task(task_id='load_to_warehouse', dag=dag)
def load_to_snowflake(agg_df):
    conn = f'''
        snowflake://{os.getenv('SNOWFLAKE_USER')}:{os.getenv('SNOWFLAKE_PASSWORD')}@
        {os.getenv('SNOWFLAKE_ACCOUNT')}/{os.getenv('SNOWFLAKE_DATABASE')}/{os.getenv('SNOWFLAKE_SCHEMA')}
    '''
    engine = sqlalchemy.create_engine(conn)
    agg_df.toPandas().to_sql('city_dim', engine, if_exists='replace')

    cursor = conn.cursor()
    cursor.close()
    conn.close()

load_to_snowflake(transform(extract_from_s3()))