from datetime import datetime, timedelta
import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable

sys.path.insert(0, os.path.dirname(__file__))

from extract import extract
from transform import transform
from insert import load

NOTIFY_EMAIL = Variable.get("JUMIA_NOTIFY_EMAIL", default_var="you@example.com")
PAGES = int(Variable.get("JUMIA_PAGES", default_var=3))

default_args = {
    "owner": "jumia_scraper",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


def run_extract(**context):
    pages = extract(max_pages=PAGES)
    if not pages:
        raise ValueError("Extract returned 0 pages")
    context["ti"].xcom_push(key="pages", value=pages)


def run_transform(**context):
    pages = context["ti"].xcom_pull(key="pages", task_ids="extract")
    if not pages:
        raise ValueError("No pages received from extract task")
    products = transform(pages)
    if not products:
        raise ValueError("Transform returned 0 products")
    context["ti"].xcom_push(key="products", value=products)


def run_load(**context):
    products = context["ti"].xcom_pull(key="products", task_ids="transform")
    if not products:
        raise ValueError("No products received from transform task")
    load(products)
    context["ti"].xcom_push(key="product_count", value=len(products))


with DAG(
    dag_id="jumia_price_scraper",
    description="Daily Jumia Home & Office price ETL",
    default_args=default_args,
    start_date=datetime(2026, 2, 25),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["jumia", "scraper", "etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=run_extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=run_transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=run_load,
    )


    success_email_task = EmailOperator(
        task_id="send_success_email",
        to=NOTIFY_EMAIL,
        subject=" Jumia Scraper — {{ ds }} run succeeded",
        html_content="{{ ti.xcom_pull(key='success_email_body', task_ids='build_success_email') }}",
    )

    failure_email_task = EmailOperator(
        task_id="send_failure_email",
        to=NOTIFY_EMAIL,
        subject="Jumia Scraper — {{ ds }} run FAILED",
        trigger_rule="one_failed",
    )

    extract_task >> transform_task >> load_task >> success_email_task
    [extract_task, transform_task, load_task] >> failure_email_task