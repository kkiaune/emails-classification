import logging
import os
import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from minio import Minio
from tempfile import NamedTemporaryFile
from sklearn.externals import joblib
from pathlib import Path
from sklearn.feature_extraction.text import TfidfVectorizer

def get_df_from_db(minio_client): 
    logging.info("Getting df from db")
    pwd = os.environ["SHARED_PASSWORD"]
    con = create_engine("postgres://shared:{pwd}@postgres/shared".format(**locals()))
    sql = """SELECT * FROM data """
    df = pd.read_sql(sql=sql, con=con)
    df['analyze'] = df["subject"].fillna('') + " " + df["body"].fillna('')
    return df;

def get_features_and_labels(df):
    logging.info("Getting features and labels")
    tfidf = TfidfVectorizer(sublinear_tf=True, min_df=5, norm='l2', encoding='latin-4', ngram_range=(1, 2))
    features = tfidf.fit_transform(df.analyze)
    labels = df.category_id
    return tfidf, features, labels;

def store_object_to_minio(minio_client, obj, bucket_name, obj_name):
    with NamedTemporaryFile() as tmp:
        logging.info("Storing new object:{obj_name} to Minio".format(**locals()))
        joblib.dump(value=obj, filename=tmp.name)
        minio_client.fput_object(
            bucket_name=bucket_name,
            object_name=obj_name,
            file_path=Path(tmp.name)
        )

def retrain_model():
    logging.info("Retraining model DAG started")

    # create minio connection
    minio_client = Minio(
        endpoint="minio:9000",
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False
    )

    # load existing model
    with NamedTemporaryFile() as tmp:
        minio_client.fget_object("models", "linearSVC.pkl", tmp.name)
        model = joblib.load(tmp.name)

    # get new data from DB
    df = get_df_from_db(minio_client)
    category_id_df = df[['category', 'category_id']].drop_duplicates().sort_values('category_id')
    # get features and labels for model training
    tfidf, features, labels = get_features_and_labels(df)
    logging.info("Training model")
    # train model
    model.fit(features, labels)
    
    # store new model
    now_formated = datetime.today().strftime('%Y-%m-%d')
    store_object_to_minio(minio_client, model, "models", "linearSVC-{now_formated}.pkl".format(**locals()))
    store_object_to_minio(minio_client, tfidf, "features", "tfidf-{now_formated}.pkl".format(**locals()))
    store_object_to_minio(minio_client, category_id_df, "categories", "categories-map-{now_formated}.pkl".format(**locals()))
    
    logging.info("Retraining model DAG finished")

### DAG ###
def get_next_sunday(startdate, weekday):
    d = datetime.strptime(startdate, '%Y-%m-%d')
    t = timedelta((7 + weekday - d.weekday()) % 7)
    return (d + t).strftime('%Y-%m-%d')

default_args={
        "owner":"airflow",
        "depends_on_past":False,
        "start_date":get_next_sunday(datetime.today().strftime('%Y-%m-%d'),6),
        'schedule_interval': '@weekly'
    }

dag = DAG(
    "dag_retrain_model", 
    default_args=default_args)

task = PythonOperator(
    task_id="task_retrain_model",
    python_callable=retrain_model,
    dag=dag,
)