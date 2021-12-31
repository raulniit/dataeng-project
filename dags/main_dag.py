import airflow
from datetime import datetime, timedelta
import pandas as pd
import json
import pyarrow
import urllib.request as request
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.neo4j.operators.neo4j import Neo4jOperator
import re
import psycopg2
from collections import Counter
# from pytrends.request import TrendReq

### Functions and variables ###

### KYM ###

def get_kym_data(url):
    request.urlretrieve(url=url, filename="/opt/airflow/dags/kym.json")
    print('File downloaded successfully.')

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

def get_vision_data(url):
    request.urlretrieve(url=url, filename= "/opt/airflow/dags/kym_vision.json")
    print('File downloaded successfully.')

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

def get_spotlight_data(url):
    request.urlretrieve(url=url, filename="/opt/airflow/dags/kym_spotlight.json")
    print('File downloaded successfully.')

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
    final = final.replace({"'": "''"}, regex = True)
    final['annot_array'] = final['annot_array'].astype("string").str.replace("'", "")
    final['types_DBpedia'] = final['types_DBpedia'].astype("string").str.replace("'", "")
    final['types_Schema'] = final['types_Schema'].astype("string").str.replace("'", "")
    final.to_parquet("/opt/airflow/dags/data_cleansed.parquet")


### RELATIONAL DB ###

def fact_table_maker (data):
    facts = data[["added_to_kym", "origin", "type", "tags"]]
    facts["added_to_kym"] = facts["added_to_kym"].fillna("0000-00-00")
    facts["type"] = facts["type"].fillna("No type")
    types = facts["type"].str.split(",", expand=True)
    types.columns = ["type" + str(i) for i in types.columns]
    facts["tags"] = facts["tags"].fillna("No tag")
    tags = facts["tags"].str.split(",", expand=True)
    tags.columns = ["tag" + str(i) for i in tags.columns]
    fact_table = pd.concat([facts, types, tags], axis=1)
    fact_table = fact_table.drop(["type", "tags"], axis=1)
    fact_table = pd.melt(fact_table, id_vars=["added_to_kym", "origin", "type0", "type1", "type2", "type3", "type4", "type5", "type6"])
    fact_table = fact_table.drop(["variable"], axis=1)
    fact_table = fact_table[fact_table.value.notnull()]
    fact_table = fact_table.rename(columns={'value': 'tag'})
    fact_table = pd.melt(fact_table, id_vars=["added_to_kym", "origin", "tag"])
    fact_table = fact_table.drop(["variable"], axis=1)
    fact_table = fact_table[fact_table.value.notnull()]
    fact_table = fact_table.rename(columns={'value': 'type'})
    fact_table["count"] = 1
    fact_table = fact_table.groupby(["added_to_kym", "origin", "tag", "type"], as_index=False, dropna=False).sum()
    return fact_table

def date_dimension_maker (fact_table):
    date_dimension = fact_table[["added_to_kym"]].drop_duplicates()
    date_dimension = date_dimension.reset_index(drop=True)
    date_dimension['date_id'] = date_dimension.index
    date_dimension[["Year", "Month", "Date"]] = date_dimension["added_to_kym"].str.split("-", expand=True)
    months = [datetime.strptime(str(x), "%m").strftime("%B") for x in date_dimension["Month"][1:]]
    months = ["None"] + months
    date_dimension["MonthName"] = months
    return date_dimension

def origin_dimension_maker (fact_table):
    origin_dimension = fact_table[["origin"]].drop_duplicates()
    origin_dimension = origin_dimension.reset_index(drop=True)
    origin_dimension['origin_id'] = origin_dimension.index
    return origin_dimension

def type_dimension_maker (fact_table):
    type_dimension = fact_table[["type"]].drop_duplicates()
    type_dimension = type_dimension.reset_index(drop=True)
    type_dimension['type_id'] = type_dimension.index
    return type_dimension

def tag_dimension_maker (fact_table):
    tag_dimension = fact_table[["tag"]].drop_duplicates()
    tag_dimension = tag_dimension.reset_index(drop=True)
    tag_dimension['tag_id'] = tag_dimension.index
    return tag_dimension

def finalize_fact_table(fact_table, date_dimension, origin_dimension, type_dimension, tag_dimension):
    fact_table = fact_table.merge(date_dimension, how="left", on="added_to_kym")
    fact_table = fact_table.merge(origin_dimension, how="left", on="origin")
    fact_table = fact_table.merge(type_dimension, how="left", on="type")
    fact_table = fact_table.merge(tag_dimension, how="left", on="tag")
    fact_table = fact_table.drop(columns=["added_to_kym", "origin", "tag", "type"])
    fact_table = fact_table.sort_values(by=['count'])
    return fact_table

def create_tables(path):
    data = pd.read_parquet(path, engine='pyarrow')
    fact_table = fact_table_maker(data)
    date_dimension = date_dimension_maker(fact_table)
    origin_dimension = origin_dimension_maker(fact_table)
    type_dimension = type_dimension_maker(fact_table)
    tag_dimension = tag_dimension_maker(fact_table)
    fact_table = finalize_fact_table(fact_table, date_dimension, origin_dimension, type_dimension, tag_dimension)
    additional_analysis_table = data.drop(columns=["url", "last_update_in_kym", "source", "origin", "year", "about", "parent", "siblings", "children"])
    additional_analysis_table = additional_analysis_table.rename(columns={"@text": "spotlight_text"})
    graph_table = data[["url", "title", "parent", "siblings", "children"]]
    fact_table.to_parquet("/opt/airflow/dags/fact_table.parquet")
    date_dimension.to_parquet("/opt/airflow/dags/date_dimension.parquet")
    origin_dimension.to_parquet("/opt/airflow/dags/origin_dimension.parquet")
    type_dimension.to_parquet("/opt/airflow/dags/type_dimension.parquet")
    tag_dimension.to_parquet("/opt/airflow/dags/tag_dimension.parquet")
    additional_analysis_table.to_parquet("/opt/airflow/dags/additional_analysis_table.parquet")
    graph_table.to_parquet("/opt/airflow/dags/graph_table.parquet")

### RELATIONAL DB ###

### POSTGRES ###

def create_date_dimension_query(path):
    data = pd.read_parquet(path, engine='pyarrow')
    with open("/opt/airflow/dags/date_dimension_inserts.sql", "w") as f:
        df_iterable = data.iterrows()
        f.write(
            "CREATE TABLE IF NOT EXISTS date_dimension (\n"
            "date_id INT PRIMARY KEY,\n"
            "Year INT,\n"
            "Month INT,\n"
            "Date INT,\n"
            "MonthName TEXT);"
        )
        for index, row in df_iterable:
            date_id = row['date_id']
            year = row['Year']
            month = row['Month']
            date = row['Date']
            monthname = row['MonthName']

            f.write(
                "INSERT INTO date_dimension VALUES ("
                f"'{date_id}', '{year}', '{month}', '{date}', '{monthname}'"
                ") ON CONFLICT (date_id) DO NOTHING;\n"
            )

        f.close()

def create_origin_dimension_query(path):
    data = pd.read_parquet(path, engine='pyarrow')
    with open("/opt/airflow/dags/origin_dimension_inserts.sql", "w") as f:
        df_iterable = data.iterrows()
        f.write(
            "CREATE TABLE IF NOT EXISTS origin_dimension (\n"
            "origin_id INT PRIMARY KEY,\n"
            "origin TEXT);"
        )
        for index, row in df_iterable:
            origin_id = row['origin_id']
            origin = row['origin']

            f.write(
                "INSERT INTO origin_dimension VALUES ("
                f"'{origin_id}', '{origin}'"
                ") ON CONFLICT (origin_id) DO NOTHING;\n"
            )

        f.close()

def create_type_dimension_query(path):
    data = pd.read_parquet(path, engine='pyarrow')
    with open("/opt/airflow/dags/type_dimension_inserts.sql", "w") as f:
        df_iterable = data.iterrows()
        f.write(
            "CREATE TABLE IF NOT EXISTS type_dimension (\n"
            "type_id INT PRIMARY KEY,\n"
            "meme_type TEXT);"
        )
        for index, row in df_iterable:
            type_id = row['type_id']
            type = row['type']

            f.write(
                "INSERT INTO type_dimension VALUES ("
                f"'{type_id}', '{type}'"
                ") ON CONFLICT (type_id) DO NOTHING;\n"
            )

        f.close()

def create_tag_dimension_query(path):
    data = pd.read_parquet(path, engine='pyarrow')
    with open("/opt/airflow/dags/tag_dimension_inserts.sql", "w") as f:
        df_iterable = data.iterrows()
        f.write(
            "CREATE TABLE IF NOT EXISTS tag_dimension (\n"
            "tag_id INT PRIMARY KEY,\n"
            "tag TEXT);"
        )
        for index, row in df_iterable:
            tag_id = row['tag_id']
            tag = row['tag']

            f.write(
                "INSERT INTO tag_dimension VALUES ("
                f"'{tag_id}', '{tag}'"
                ")ON CONFLICT (tag_id) DO NOTHING;\n"
            )

        f.close()


def create_fact_table_query(path):
    data = pd.read_parquet(path, engine='pyarrow')
    with open("/opt/airflow/dags/fact_table_inserts.sql", "w") as f:
        df_iterable = data.iterrows()
        f.write(
            "CREATE TABLE IF NOT EXISTS fact_table (\n"
            "date_id INT REFERENCES date_dimension (date_id),\n"
            "origin_id INT REFERENCES origin_dimension (origin_id),\n"
            "type_id INT REFERENCES type_dimension (type_id),\n"
            "tag_id INT REFERENCES tag_dimension (tag_id),\n"
            "count INT);\n"
        )
        for index, row in df_iterable:
            date_id = row['date_id']
            origin_id = row['origin_id']
            type_id = row['type_id']
            tag_id = row['tag_id']
            count= row['count']

            f.write(
                "INSERT INTO fact_table VALUES ("
                f"'{date_id}', '{origin_id}', '{type_id}', '{tag_id}', '{count}'"
                ");\n"
            )

        f.close()

def create_additional_analysis_table_query(path):
    data = pd.read_parquet(path, engine='pyarrow')
    with open("/opt/airflow/dags/additional_analysis_table_inserts.sql", "w") as f:
        df_iterable = data.iterrows()
        f.write(
            "CREATE TABLE IF NOT EXISTS additional_analysis_table (\n"
            "title TEXT,\n"
            "description TEXT,\n"
            "added_to_kym TEXT,\n"
            "meme_type TEXT,\n"
            "spread_counts INT,\n"
            "spread_year_min FLOAT ,\n"
            "spread_year_max FLOAT,\n"
            "origin_references TEXT,\n"
            "spread_references TEXT,\n"
            "tags TEXT,\n"
            "search_keywords TEXT,\n"
            "annot_array TEXT ,\n"
            "safeSearch_adult FLOAT,\n"
            "safeSearch_medical FLOAT,\n"
            "safeSearch_racy FLOAT,\n"
            "safeSearch_spoof FLOAT,\n"
            "safeSearch_violence FLOAT,\n"
            "webDetection_bestGuessLabel TEXT,\n"
            "spotlight_text TEXT,\n"
            "types_DBpedia TEXT,\n"
            "types_Schema TEXT);\n"
        )
        for index, row in df_iterable:
            title = row['title']
            description = row['description']
            added_to_kym = row['added_to_kym']
            type = row['type']
            spread_counts = row['spread_counts']
            spread_year_min = row['spread_year_min']
            spread_year_max = row['spread_year_max']
            origin_references = row['origin_references']
            spread_references = row['spread_references']
            tags = row['tags']
            search_keywords = row['search_keywords']
            annot_array = row['annot_array']
            safeSearch_adult = row['safeSearch_adult']
            safeSearch_medical = row['safeSearch_medical']
            safeSearch_racy = row['safeSearch_racy']
            safeSearch_spoof = row['safeSearch_spoof']
            safeSearch_violence = row['safeSearch_violence']
            webDetection_bestGuessLabel = row['webDetection_bestGuessLabel']
            spotlight_text = row['spotlight_text']
            types_DBpedia = row['types_DBpedia']
            types_Schema = row['types_Schema']

            f.write(
                "INSERT INTO additional_analysis_table VALUES ("
                f"'{title}', '{description}', '{added_to_kym}', '{type}', '{spread_counts}', '{spread_year_min}', '{spread_year_max}', '{origin_references}', '{spread_references}', '{tags}', '{search_keywords}', '{annot_array}', '{safeSearch_adult}', '{safeSearch_medical}', '{safeSearch_racy}', '{safeSearch_spoof}', '{safeSearch_violence}', '{webDetection_bestGuessLabel}', '{spotlight_text}', '{types_DBpedia}', '{types_Schema}'"
                ");\n"
            )

        f.close()

### POSTGRES ###

### NEO4J ###

def create_graph_table_query(path):
    data = pd.read_parquet(path, engine='pyarrow')
    with open("/opt/airflow/dags/graph_table_inserts.sql", "w") as f:
        df_iterable = data.iterrows()
        for index, row in df_iterable:
            url = row['url']
            title = row['title'].replace(" ", "")
            f.write(
                f"CREATE ({title}:meme{{title:\"{title}\", url:\"{url}\"}})\n"
            )
        for index, row in df_iterable:
            if not pd.isna(row["parent"]):
                parent = data[data["url"] == row["parent"]].title
                if not parent.empty:
                    title = row['title'].replace(" ", "")
                    parent_title = parent.to_string().replace(" ", "")
                    f.write(
                        f"CREATE ({parent_title})-[:parent_to]->({title})\n"
                    )

            if not pd.isna(row["siblings"]):
                siblings = row["siblings"].split(",")
                for sibling in siblings:
                    sib = data[data["url"] == sibling].title
                    if not sib.empty:
                        title = row['title'].replace(" ", "")
                        sib_title = sib.to_string().replace(" ", "")
                        f.write(
                            f"CREATE ({sib_title})-[:sibling_to]->({title})\n"
                        )

            if not pd.isna(row["children"]):
                children = row["children"].split(",")
                for child in children:
                    chi = data[data["url"] == child].title
                    if not chi.empty:
                        title = row['title'].replace(" ", "")
                        chi_title = chi.to_string().replace(" ", "")
                        f.write(
                            f"CREATE ({chi_title})-[:child_to]->({title})\n"
                        )

        f.close()


### NEO4J ###

default_args_dict = {
    'start_date': datetime(2021, 11, 13, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval' : timedelta(minutes=5),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

### Functions and variables ###


### PIPELINE ###

main_dag = DAG(
    dag_id="main_dag",
    default_args=default_args_dict,
    catchup=False,
)

task_one = PythonOperator(
    task_id='download_kym_data',
    dag=main_dag,
    python_callable=get_kym_data,
    op_kwargs={
        "url": "https://owncloud.ut.ee/owncloud/index.php/s/g4qB5DZrFEz2XLm/download/kym.json"
    }
)

task_three_a = PythonOperator(
    task_id='download_vision_data',
    dag=main_dag,
    python_callable=get_vision_data,
    op_kwargs={
        "url": "https://owncloud.ut.ee/owncloud/index.php/s/teoFdWKBzzqcFjY/download/kym_vision.json"
    }
)

task_three_b = PythonOperator(
    task_id='download_spotlight_data',
    dag=main_dag,
    python_callable=get_spotlight_data,
    op_kwargs={
        "url": "https://owncloud.ut.ee/owncloud/index.php/s/iMM8crN4AKSpFZZ/download/kym_spotlight.json"
    }
)


task_two = PythonOperator(
    task_id="cleanse_kym",
    dag=main_dag,
    python_callable=cleanse_kym,
    op_kwargs={"path": "/opt/airflow/dags/kym.json"},
)

task_four_a = PythonOperator(
    task_id="cleanse_vision",
    dag=main_dag,
    python_callable=cleanse_vision,
    op_kwargs={"path": "/opt/airflow/dags/kym_vision.json"},
)

task_four_b = PythonOperator(
    task_id="cleanse_spotlight",
    dag=main_dag,
    python_callable=cleanse_spotlight,
    op_kwargs={"path": "/opt/airflow/dags/kym_spotlight.json"},
)

task_five = PythonOperator(
    task_id="merge",
    dag=main_dag,
    python_callable=merge_all,
    op_kwargs={"path1": "/opt/airflow/dags/kym_processed.parquet",
               "path2": "/opt/airflow/dags/kym_vision_processed.parquet",
               "path3": "/opt/airflow/dags/kym_spotlight_processed.parquet"},
)

task_six = PythonOperator(
    task_id="create_tables",
    dag=main_dag,
    python_callable=create_tables,
    op_kwargs={"path": "/opt/airflow/dags/data_cleansed.parquet"},
)

task_seven_a = PythonOperator(
    task_id="create_date_dimension_query",
    dag=main_dag,
    python_callable=create_date_dimension_query,
    op_kwargs={"path": "/opt/airflow/dags/date_dimension.parquet"},
)

task_seven_b = PythonOperator(
    task_id="create_origin_dimension_query",
    dag=main_dag,
    python_callable=create_origin_dimension_query,
    op_kwargs={"path": "/opt/airflow/dags/origin_dimension.parquet"},
)

task_seven_c = PythonOperator(
    task_id="create_type_dimension_query",
    dag=main_dag,
    python_callable=create_type_dimension_query,
    op_kwargs={"path": "/opt/airflow/dags/type_dimension.parquet"},
)
task_seven_d = PythonOperator(
    task_id="create_tag_dimension_query",
    dag=main_dag,
    python_callable=create_tag_dimension_query,
    op_kwargs={"path": "/opt/airflow/dags/tag_dimension.parquet"},
)

task_seven_e = PythonOperator(
    task_id="create_fact_table_query",
    dag=main_dag,
    python_callable=create_fact_table_query,
    op_kwargs={"path": "/opt/airflow/dags/fact_table.parquet"},
)

task_seven_f = PythonOperator(
    task_id="create_additional_analysis_table_query",
    dag=main_dag,
    python_callable=create_additional_analysis_table_query,
    op_kwargs={"path": "/opt/airflow/dags/additional_analysis_table.parquet"},
)

task_seven_i = PythonOperator(
    task_id="create_graph_table_query",
    dag=main_dag,
    python_callable=create_graph_table_query,
    op_kwargs={"path": "/opt/airflow/dags/graph_table.parquet"},
)

task_eight_a = PostgresOperator(
    task_id='insert_date_dimension_query',
    dag=main_dag,
    postgres_conn_id='postgres_default',
    sql='date_dimension_inserts.sql',
    trigger_rule='all_success',
    autocommit=True
)

task_eight_b = PostgresOperator(
    task_id='insert_origin_dimension_query',
    dag=main_dag,
    postgres_conn_id='postgres_default',
    sql='origin_dimension_inserts.sql',
    trigger_rule='all_success',
    autocommit=True
)

task_eight_c = PostgresOperator(
    task_id='insert_type_dimension_query',
    dag=main_dag,
    postgres_conn_id='postgres_default',
    sql='type_dimension_inserts.sql',
    trigger_rule='all_success',
    autocommit=True
)

task_eight_d = PostgresOperator(
    task_id='insert_tag_dimension_query',
    dag=main_dag,
    postgres_conn_id='postgres_default',
    sql='tag_dimension_inserts.sql',
    trigger_rule='all_success',
    autocommit=True
)

task_eight_e = PostgresOperator(
    task_id='insert_fact_table_query',
    dag=main_dag,
    postgres_conn_id='postgres_default',
    sql='fact_table_inserts.sql',
    trigger_rule='all_success',
    autocommit=True
)

task_eight_f = PostgresOperator(
    task_id='insert_additional_analysis_table_query',
    dag=main_dag,
    postgres_conn_id='postgres_default',
    sql='additional_analysis_table_inserts.sql',
    trigger_rule='all_success',
    autocommit=True
)

task_eight_i = Neo4jOperator(
    task_id='insert_graph_table_query',
    dag=main_dag,
    neo4j_conn_id='neo4j_default',
    sql="graph_table.sql",
    trigger_rule='all_success'
)

# task_nine_a = PostgresOperator(
#     task_id='request1',
#     dag=main_dag,
#     postgres_conn_id='postgres_default',
#     sql='SELECT * FROM fact_table;',
#     trigger_rule='all_success',
#     autocommit=True
# )
#
# task_nine_b = PostgresOperator(
#     task_id='request2',
#     dag=main_dag,
#     postgres_conn_id='postgres_default',
#     sql='SELECT * FROM additional_analysis_table;',
#     trigger_rule='all_success',
#     autocommit=True
# )


task_one >> task_two
task_two >> [task_three_a, task_three_b]
task_three_a >> task_four_a
task_three_b >> task_four_b
[task_four_a, task_four_b] >> task_five
task_five >> task_six >> [task_seven_a, task_seven_b, task_seven_c, task_seven_d, task_seven_f, task_seven_i]
task_seven_a >> task_eight_a
task_seven_b >> task_eight_b
task_seven_c >> task_eight_c
task_seven_d >> task_eight_d
task_seven_f >> task_eight_f
task_seven_i >> task_eight_i
[task_eight_a, task_eight_b, task_eight_c, task_eight_d] >> task_seven_e >> task_eight_e


### PIPELINE ###
