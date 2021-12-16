import airflow
import datetime
import pandas as pd
import json
import pyarrow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

### Functions and variables ###

safeSearchToNumberCode = {
    "VERY_UNLIKELY": 0.0,
    "UNLIKELY": 0.25,
    "POSSIBLE": 0.5,
    "LIKELY": 0.75,
    "VERY_LIKELY": 1.0
}

def getSafeSearch(row):
  if (isinstance(row.safeSearchAnnotation, dict)):
    for key in row.safeSearchAnnotation.keys():
      row["safeSearch_"+key] = safeSearchToNumberCode[row.safeSearchAnnotation[key]]
  return row

def addSafeSearchRows(path):
    df = pd.read_json(path, orient="index")
    df["url"] = df.index
    df = df.reset_index()
    df = df.drop(columns=["index"])
    df.apply(getSafeSearch, axis=1)
    df = df.drop(columns=["safeSearchAnnotation"])
    df.to_parquet("kym_vision_processed.parquet")


def writeOutToParquet(path):
    df = pd.read_parquet(path, engine='pyarrow')
    df.to_parquet("kym_vision_processed2.parquet")

default_args_dict = {
    'start_date': datetime.datetime(2021, 11, 13, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval' : datetime.timedelta(minutes=5),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=10),
}

### Functions and variables ###


### PIPELINE ###

first_dag = DAG(
    dag_id="first_dag",
    default_args=default_args_dict,
    catchup=False,
)

task_one = PythonOperator(
    task_id="addSafeSearchRows",
    dag=first_dag,
    python_callable=addSafeSearchRows,
    op_kwargs={"path": "/opt/airflow/dags/kym_vision.json"},
)

task_two = PythonOperator(
    task_id="writeOut",
    dag=first_dag,
    python_callable=writeOutToParquet,
    op_kwargs={"path": "/opt/airflow/dags/kym_vision_processed.parquet"},
    trigger_rule='all_success',
    depends_on_past=True,
)

task_one >> task_two

### PIPELINE ###
