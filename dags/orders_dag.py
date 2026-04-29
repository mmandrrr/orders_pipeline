"""Orders pipeline DAG"""

from datetime import datetime
from sqlalchemy import create_engine
import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


def extract(**context):
    DB_PASS = Variable.get("DB_PASSWORD")
    # line for local testing with docker
    # engine = create_engine(
    #     f"postgresql://postgres:{DB_PASS}@host.docker.internal:5432/music_db"
    # )
    engine = create_engine(f"postgresql://postgres:{DB_PASS}@localhost:5432/music_db")
    df = pd.read_sql_query("SELECT * FROM orders", engine)
    df["created_at"] = df["created_at"].astype(str)
    context["ti"].xcom_push(key="data", value=df.to_dict(orient="records"))


def transform(**context):
    df = pd.DataFrame(context["ti"].xcom_pull(key="data", task_ids="extract"))
    df["amount_uah"] = df["amount"] * 43.5
    df = df[df["amount"] > 100]
    df["created_at"] = pd.to_datetime(df["created_at"])
    df["date"] = df["created_at"].dt.date
    df["date"] = df["date"].astype(str)
    df["created_at"] = df["created_at"].astype(str)
    context["ti"].xcom_push(key="data", value=df.to_dict(orient="records"))


def load(**context):
    bucket = "orders-pipeline-my-mini-proj"
    key = "orders/orders_transformed.csv"

    df = pd.DataFrame(context["ti"].xcom_pull(key="data", task_ids="transform"))
    csv_data = df.to_csv(index=False)

    s3 = boto3.client(
        "s3",
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
        region_name=Variable.get("AWS_REGION"),
    )

    s3.put_object(Bucket=bucket, Key=key, Body=csv_data)
    print("Loaded successfully!")


with DAG(
    dag_id="orders_dag",
    start_date=datetime(2026, 4, 29),
    schedule="@daily",
    catchup=False,
) as orders:
    task_extract = PythonOperator(task_id="extract", python_callable=extract)
    task_transform = PythonOperator(task_id="transform", python_callable=transform)
    task_load = PythonOperator(task_id="load", python_callable=load)

    task_extract >> task_transform >> task_load
