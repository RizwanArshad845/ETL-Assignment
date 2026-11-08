"""Daily ETL: Frankfurter exchange rates -> transform -> MongoDB.

Tasks pass data with *named* XComs (explicit keys) instead of the default return_value.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from pymongo import UpdateOne

from include.currency_etl import extract_rates, transform_rates

MONGO_CONN_ID = "mongo_default"
MONGO_DB = "currency_db"
MONGO_COLLECTION = "exchange_rates"

RAW_KEY = "raw_exchange_rates"
TRANSFORMED_KEY = "transformed_exchange_rates"


def extract(ti, **_):
    raw = extract_rates()
    ti.xcom_push(key=RAW_KEY, value=raw)
    print(f"Extracted {len(raw)} rates: {raw}")


def transform(ti, **_):
    raw = ti.xcom_pull(task_ids="extract_exchange_rates", key=RAW_KEY)
    if not raw:
        raise ValueError(f"No data found in XCom key '{RAW_KEY}'")
    transformed = transform_rates(raw)
    ti.xcom_push(key=TRANSFORMED_KEY, value=transformed)
    print(f"Transformed {len(transformed)} records: {transformed}")


def load(ti, **_):
    records = ti.xcom_pull(task_ids="transform_exchange_rates", key=TRANSFORMED_KEY)
    if not records:
        raise ValueError(f"No data found in XCom key '{TRANSFORMED_KEY}'")

    client = MongoHook(mongo_conn_id=MONGO_CONN_ID).get_conn()
    try:
        collection = client[MONGO_DB][MONGO_COLLECTION]
        # One document per currency pair per day; reruns update instead of duplicating.
        collection.create_index(
            [("base_currency", 1), ("target_currency", 1), ("rate_date", 1)], unique=True
        )
        ops = [
            UpdateOne(
                {k: r[k] for k in ("base_currency", "target_currency", "rate_date")},
                {"$set": r},
                upsert=True,
            )
            for r in records
        ]
        result = collection.bulk_write(ops)
        print(
            f"Loaded into {MONGO_DB}.{MONGO_COLLECTION}: "
            f"inserted={result.upserted_count}, updated={result.modified_count}"
        )
    finally:
        client.close()


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="currency_exchange_etl",
    description="Daily EUR exchange rates from Frankfurter API into MongoDB",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["etl", "currency", "mongodb"],
) as dag:
    extract_task = PythonOperator(task_id="extract_exchange_rates", python_callable=extract)
    transform_task = PythonOperator(task_id="transform_exchange_rates", python_callable=transform)
    load_task = PythonOperator(task_id="load_to_mongodb", python_callable=load)

    extract_task >> transform_task >> load_task
