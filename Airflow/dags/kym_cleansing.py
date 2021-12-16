import airflow
from datetime import datetime, timedelta
import pandas as pd
import json
import pyarrow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import re
from collections import Counter
# from pytrends.request import TrendReq

### Functions and variables ###

### KYM ###

def cleanse_kym(path):

    with open(path, 'r', encoding='utf-8') as reader:
        memes = json.load(reader)

    cleaned_memes = []

    for meme in memes:
        meme_dict = {}
        if meme['category'] != 'Meme':
            continue

        if meme['details']['status'] != 'confirmed':
            continue

        if meme['details'].get('type', None):
            if set(map(lambda link: link.split('/')[-1], meme['details']['type'])) \
                    & {'crime', 'disaster', 'exploitable', 'fetish', 'religion'}:
                continue

        if meme['title'] in list(map(lambda entry: entry['title'], cleaned_memes)):
            cleaned_meme = list(filter(lambda entry: entry['title'] == meme['title'], cleaned_memes))[-1]
            if datetime.fromtimestamp(meme['last_update_source']) > datetime.fromisoformat(
                    cleaned_meme['last_update_in_kym']):
                cleaned_memes.remove(cleaned_meme)
            elif datetime.fromtimestamp(meme['last_update_source']) == datetime.fromisoformat(
                    cleaned_meme['last_update_in_kym']):
                continue
            else:
                continue

        meme_dict['title'] = meme['title']
        meme_dict['kym_url'] = meme['url']
        meme_dict['last_update_in_kym'] = datetime.fromtimestamp(meme['last_update_source']).strftime(
            '%Y-%m-%d %H:%M:%S')
        meme_dict['description'] = meme['meta']['description']
        meme_dict['source'] = 'Know Your Meme'
        meme_dict['added_to_kym'] = datetime.fromtimestamp(meme['added']).strftime('%Y-%m-%d') \
            if meme.get('added', None) else None

        meme_dict['origin'] = meme['details']['origin']

        if meme['details'].get('year', None):
            if 1993 <= int(meme['details']['year']) < 2022:
                meme_dict['year'] = int(meme['details']['year'])
            else:
                meme_dict['year'] = None
        else:
            meme_dict['year'] = None

        meme_dict['type'] = ",".join(list(map(lambda link: link.split('/')[-1], meme['details']['type']))) \
            if meme['details'].get('type', None) else None
        try:
            meme_dict['about'] = " ".join(meme['content']['about']['text'])
        except KeyError:
            meme_dict['about'] = None

        try:
            origin_references = re.findall(r'\[\d+\]', "".join(meme['content']['origin']['text']))
            spread_references = re.findall(r'\[\d+\]', "".join(meme['content']['spread']['text']))
            meme_dict['spread_counts'] = len(
                spread_references)  # count number of external references appeared in spread description

            # find all occurences of dates, year specifically, mentioned in spread description
            spread_years = re.findall(r'(199\d|20[0-1]\d|202[0-1])', "".join(meme['content']['spread']['text']))
            if spread_years:
                meme_dict['spread_year_min'] = min(list(map(int, spread_years)))
                meme_dict['spread_year_max'] = max(list(map(int, spread_years)))
            else:
                meme_dict['spread_year_min'] = None
                meme_dict['spread_year_max'] = None

            # build a dictionary of external references
            # key - reference number in brackets, value - reference name
            exrefs_dict = {}
            for entry in meme['content']['external references']['text']:
                try:
                    num, name = entry.split('] ')
                    exrefs_dict[num + ']'] = name.split(' \u2013 ')[0].strip()
                except ValueError:  # not enough values to unpack
                    continue

            # put together all reference names mentioned in the origin section into a list
            origin_names = []
            for ref in origin_references:
                origin_names.append(exrefs_dict[ref])

            # put together all reference names mentioned in the spread section into a list
            spread_names = []
            for ref in spread_references:
                spread_names.append(exrefs_dict[ref])

            meme_dict['origin_references'] = ",".join(set(origin_names))
            meme_dict['spread_references'] = ",".join(set(spread_names))
        except KeyError:
            meme_dict['spread_counts'] = 0
            meme_dict['spread_references'] = None
            meme_dict['origin_references'] = None

        meme_dict['tags'] = ",".join(meme['tags']) if meme.get('tags', None) else None
        meme_dict['search_keywords'] = ",".join(meme['search_keywords']) if meme.get('search_keywords', None) else None

        meme_dict['parent'] = meme.get('parent', None)
        meme_dict['siblings'] = ",".join(meme['siblings']) if meme.get('siblings', None) else None
        meme_dict['children'] = ",".join(meme['children']) if meme.get('children', None) else None

        cleaned_memes.append(meme_dict)

        pd.DataFrame(cleaned_memes).to_parquet("/opt/airflow/dags/kym_processed.parquet")

### KYM ###

### VISION ###

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

def getLabelAnnotScores(row):
  if ((not type(row.labelAnnotations) == float) and len(row.labelAnnotations) > 0):
    #newDict = {annot["description"]: annot["score"] for annot in row.labelAnnotations}
    #row["annot_array"] = list(newDict.keys())
    row["annot_array"] = [annot["description"] for annot in row.labelAnnotations]
  return row

def getBestGuessLabel(row):
  if ((type(row.webDetection) == dict)):
    # and "bestGuessLabels" in row.webDetection and len(row.webDetection.bestGuessLabels) > 0):
    bestGuessLabels = row.webDetection.get("bestGuessLabels", [])
    if (len(bestGuessLabels) > 0 and 'label' in bestGuessLabels[0]):
      row["webDetection_bestGuessLabel"] = bestGuessLabels[0]['label']
  return row

def cleanse_vision(path):
    df = pd.read_json(path, orient="index")
    df["url"] = df.index
    df = df.reset_index()
    df = df.drop(columns=["index"])
    df = df.apply(getSafeSearch, axis=1)
    df = df.drop(columns=["safeSearchAnnotation"])
    df = df.apply(getLabelAnnotScores, axis=1)
    df = df.drop(columns="labelAnnotations")
    df = df.apply(getBestGuessLabel, axis=1)
    df = df.drop(columns=["webDetection", "error"])
    df.to_parquet("/opt/airflow/dags/kym_vision_processed.parquet")

### VISION ###

### SPOTLIGHT ###

def getTypeDict(types):
  uniqueTypeList = list(set(j for t in types for j in t.split(",")))
  tDict = {}
  for t in uniqueTypeList:
    if "http:" in t.lower():
      continue
    key, value = t.split(":")
    if key in tDict:
      tDict[key].append(value)
    else:
      tDict[key] = [value]
  return tDict

def getTypes(row):
  if (pd.isna(row)["Resources"]):
    return row
  types = [x["@types"] for x in row["Resources"] if x["@types"] != ""]
  uniqueTypeList = list(set(j for t in types for j in t.split(",")))
  tDict = getTypeDict(types)
  if "DBpedia" in tDict:
    row["types_DBpedia"] = tDict["DBpedia"]
  if "Schema" in tDict:
    row["types_Schema"] = tDict["Schema"]
  return row

def cleanse_spotlight(path):
    df = pd.read_json(path, orient="index")
    df["url"] = df.index
    df = df.reset_index()
    df = df.drop(columns=["index"])
    df = df.drop(columns=["@confidence", "@support", "@types", "@sparql", "@policy"])
    df = df.apply(getTypes, axis=1)
    df = df.drop(columns=["Resources"])
    df.to_parquet("/opt/airflow/dags/kym_spotlight_processed.parquet")

### SPOTLIGHT ###

def merge_all(path1, path2, path3):
    kym = pd.read_parquet(path1, engine='pyarrow')
    vision = pd.read_parquet(path2, engine='pyarrow')
    spot = pd.read_parquet(path3, engine='pyarrow')
    kym = kym.rename(columns={"kym_url": "url"})
    final = kym.merge(vision, on="url", how="left")
    final = final.merge(spot, on="url", how = "left")
    final.to_parquet("/opt/airflow/dags/data_cleansed.parquet")



default_args_dict = {
    'start_date': datetime(2021, 11, 13, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval' : timedelta(minutes=5),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

### Functions and variables ###


### PIPELINE ###

cleansing_dag = DAG(
    dag_id="cleansing_dag",
    default_args=default_args_dict,
    catchup=False,
)

task_one = PythonOperator(
    task_id="cleanse_kym",
    dag=cleansing_dag,
    python_callable=cleanse_kym,
    op_kwargs={"path": "/opt/airflow/dags/kym.json"},
)

task_two = PythonOperator(
    task_id="cleanse_vision",
    dag=cleansing_dag,
    python_callable=cleanse_vision,
    op_kwargs={"path": "/opt/airflow/dags/kym_vision.json"},
)

task_three = PythonOperator(
    task_id="cleanse_spotlight",
    dag=cleansing_dag,
    python_callable=cleanse_spotlight,
    op_kwargs={"path": "/opt/airflow/dags/kym_spotlight.json"},
)

task_four = PythonOperator(
    task_id="merge",
    dag=cleansing_dag,
    python_callable=merge_all,
    op_kwargs={"path1": "/opt/airflow/dags/kym_processed.parquet",
               "path2": "/opt/airflow/dags/kym_vision_processed.parquet",
               "path3": "/opt/airflow/dags/kym_spotlight_processed.parquet"},
)

[task_one, task_two, task_three] >> task_four

### PIPELINE ###
