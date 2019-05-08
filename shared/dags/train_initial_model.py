import logging
import os
import re
import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from minio import Minio
from tempfile import NamedTemporaryFile
from sklearn.externals import joblib
from pathlib import Path
from sklearn.svm import LinearSVC
from sklearn.feature_extraction.text import TfidfVectorizer

### TASK1 ###
def readCsvToDB(minio_client, filename, con, if_exists):
    logging.info("Reading file: {filename} to df".format(**locals()))
    data = minio_client.get_object("data", filename)
    df = pd.read_csv(data, sep='\t')
    df["category"] = df["category"].fillna('Nenustatyta')
    df['category_id'] = df['category'].factorize()[0]
    logging.info("Storing df to DB")
    df.to_sql(name="emails", con=con, index=False, if_exists="replace")

def generate_data():
    logging.info("Generating data to DB started")
    # fetching the data
    minio_client = Minio(
        endpoint="minio:9000",
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False
    )
    pwd = os.environ["SHARED_PASSWORD"]
    con = create_engine(
        "postgres://shared:{pwd}@postgres/shared".format(**locals()))
    # change to other files
    readCsvToDB(minio_client, "EmailBox1.csv", con, "replace");
    readCsvToDB(minio_client, "EmailBox2.csv", con, "append");
    logging.info("Generating data to DB finished")
    
### TASK2 ###
def get_df_from_db(minio_client): 
    logging.info("Getting df from db")
    pwd = os.environ["SHARED_PASSWORD"]
    con = create_engine("postgres://shared:{pwd}@postgres/shared".format(**locals()))
    sql = """SELECT * FROM data"""
    df = pd.read_sql(sql=sql, con=con)
    df['analyze'] = df["subject"].fillna('') + " " + df["body"].fillna('')
    logging.info("Returning df")
    return df;

def get_features_and_labels(df):
    logging.info("Getting features and labels")
    tfidf = TfidfVectorizer(sublinear_tf=True, min_df=5, norm='l2', encoding='latin-4', ngram_range=(1, 2))
    features = tfidf.fit_transform(df.analyze)
    labels = df.category_id
    logging.info("Returning features and labels")
    return tfidf, features, labels;

def create_bucket_if_not_exists(minio_client, bucket_name):
    logging.info("Trying to create bucket: {bucket_name}".format(**locals()))
    try:
        minio_client.bucket_exists(bucket_name)
    except ResponseError as err:
        logging.info("There is no bucket, creating")
        minio_client.make_bucket(bucket_name)
    logging.info("Bucket creation finished")

def store_object_to_minio(minio_client, obj, bucket_name, obj_name):
    with NamedTemporaryFile() as tmp:
        logging.info("Storing object:{obj_name} to Minio".format(**locals()))
        create_bucket_if_not_exists(minio_client, bucket_name)
        joblib.dump(value=obj, filename=tmp.name)
        minio_client.fput_object(
            bucket_name=bucket_name,
            object_name=obj_name,
            file_path=Path(tmp.name)
        )

def train_model():
    logging.info("Training model starting")

    # create minio connection
    minio_client = Minio(
        endpoint="minio:9000",
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False
    )

    # get new data from DB
    df = get_df_from_db(minio_client)
    # get features and labels for model training
    tfidf, features, labels = get_features_and_labels(df)
    # train model
    category_id_df = df[['category', 'category_id']].drop_duplicates().sort_values('category_id')
    model = LinearSVC(penalty='l2', loss="hinge")
    logging.info("Fitting model")
    model.fit(features, labels)

    # Store data to Minio
    store_object_to_minio(minio_client, model, "models", "linearSVC.pkl")
    store_object_to_minio(minio_client, tfidf, "features", "tfidf.pkl")
    store_object_to_minio(minio_client, category_id_df, "categories", "categories-map.pkl")

    logging.info("Training model finished")

### DAG ###
default_args={
        "owner":"airflow",
        "depends_on_past":False,
        "start_date":datetime.today()-timedelta(days=1),
        'schedule_interval': '@once'
        }

dag = DAG(
    "dag_init_data_and_model", 
    default_args=default_args)

task1 = PythonOperator(
    task_id="task_generate_data",
    python_callable=generate_data,
    dag=dag,
)

task2 = PythonOperator(
    task_id="task_train_model",
    python_callable=train_model,
    dag=dag,
)

task1 >> task2