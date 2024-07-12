from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'deepak',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',  # Run once a day
}

dag = DAG(
    'daily_dataflow_job',
    default_args=default_args,
    description='Run Dataflow job daily',
    schedule_interval='0 0 * * *',  # Run at midnight UTC every day
    catchup=False,  # Skip catching up on historical DAG runs
)

date=datetime.now().strftime('%d_%m_%Y')

BODY = {
    "launchParameter": {
        "jobName": f"food-orders-pipeline",
        "parameters": {
            "input": f"gs://food_orders_delivery/data/food_daily_{date}.csv",
            "dataset": f"demodataset",
            "staging_location":"gs://food_orders_delivery/tmp/"
        },
        "environment": {},
        "containerSpecGcsPath": "gs://food_orders_delivery/batch_pipeline.json",
    }
}

start_flex_template_job = DataflowStartFlexTemplateOperator(
    task_id="start_flex_template_job",
    project_id='nimish-423515',
    body=BODY,
    location='us-central1',
    append_job_name=True,
    wait_until_finished=True,
    dag=dag
)

start_flex_template_job