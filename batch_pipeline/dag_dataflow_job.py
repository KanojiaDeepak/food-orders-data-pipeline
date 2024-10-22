from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator,DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

project_id='high-plating-431207-s0'

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

#date=datetime.now().strftime('%d_%m_%Y')
date='12_07_2024'

def success_alert(context):
    print(context)
    print(f"Task has succeeded, task_instance_key_str: {context['task_instance_key_str']}")

def failure_alert(context):
    print(context)
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")

dataflow_job = DataflowCreatePythonJobOperator(
        task_id='dataflow_job',
        py_file='gs://food_orders_delivery/batch_pipeline.py',
        job_name='my-dataflow-job',
        location='us-central1',  
        dataflow_default_options={
            'input': f"gs://food_orders_delivery/data/food_daily_{date}.csv",
            "dataset": f"demodataset",
            "staging_location":"gs://food_orders_delivery/tmp/"
        },
        dag=dag,
        gcp_conn_id='google_cloud_default',
        wait_until_finished=True,
        on_success_callback=success_alert,
        on_failure_callback=failure_alert
    )

start = DummyOperator(task_id='start',dag=dag)
end = DummyOperator(task_id='end',dag=dag)

start >> dataflow_job >> end