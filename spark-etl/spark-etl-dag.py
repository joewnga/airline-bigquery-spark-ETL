from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define default_args dictionary to pass to the DAG
default_args = {
    'owner': 'me',
    'start_date': datetime(2022, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG instance
dag = DAG(
    'opensky_etl',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)


def run_opensky_etl():
    
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *

    
    spark = SparkSession.builder.appName("OpenSky_ETL").getOrCreate()

    
    df = spark.read.format("csv").option("header", "true").load("../data/opensky_dataset.csv")

    
    df = df.selectExpr("callsign", "number", "icao24", "registration", "typecode", "origin", "destination", "firstseen", "lastseen", "day", "latitude_1 as latitude_first", "longitude_1 as longitude_first", "altitude_1 as altitude_first", "latitude_2 as latitude_last", "longitude_2 as longitude_last", "altitude_2 as altitude_last")

    
    df.write.format("bigquery").option("table", "project_id.dataset_id.fact_table_name").mode("append").save()

    
    spark.stop()


run_opensky_etl = PythonOperator(
    task_id='run_opensky_etl',
    python_callable=run_opensky_etl,
    dag=dag,
)


run_opensky_etl

