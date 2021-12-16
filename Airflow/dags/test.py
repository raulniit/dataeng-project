import airflow
import datetime
import pandas as pd
import json
from airflow.operators.python_operator import PythonOperator

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
    with open("kym_vision_processed.json", "w", encoding="utf-8") as f:
        json.dump(df.to_json(), f, ensure_ascii=False)


def writeOutToParquet(path):
    df = pd.read_json(path, orient="records", lines=True)
    df.to_parquet("kym_vision_processed.parquet")

def writeOutToParquet(path):
    df = pd.read_parquet(path, engine='pyarrow')
    df.to_parquet("kym_processed2.parquet")

# task_four = PythonOperator(
#     task_id="writeOut",
#     dag=cleansing_dag,
#     python_callable=writeOutToParquet,
#     op_kwargs={"path": "/opt/airflow/dags/kym_processed.parquet"},
#     trigger_rule='all_success',
#     depends_on_past=True,
# )

#addSafeSearchRows("kym_vision.json")
#writeOutToParquet("kym_vision_processed.json")


# df = pd.read_json("kym_vision_processed.json", orient="records", lines=True)
# df.to_parquet("kym_vision_processed.parquet")
# df2 = pd.read_parquet('kym_vision_processed.parquet', engine='pyarrow')
# df2.to_parquet("kym_vision_processed2.parquet")
#df.to_parquet("kym_vision_processed.parquet")

#writeOutToParquet("kym_vision_processed.json")