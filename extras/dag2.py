from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

project_id='high-plating-431207-s0'
bq_dataset='demodataset'
bq_error_table='reconciliation'

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
    'daily_dataflow_job_new',
    default_args=default_args,
    description='Run Dataflow job daily',
    schedule_interval='0 0 * * *',  # Run at midnight UTC every day
    catchup=False,  # Skip catching up on historical DAG runs
)

#date=datetime.now().strftime('%d_%m_%Y')
date='12_07_2024'

BODY = {
    "launchParameter": {
        "jobName": f"food-orders-pipeline-{date}",
        "parameters": {
            "input": f"gs://food_orders_delivery/data/food_daily_{date}.csv",
            "dataset": f"demodataset",
            "staging_location":"gs://food_orders_delivery/tmp/"
        },
        "environment": {},
        "containerSpecGcsPath": "gs://food_orders_delivery/batch_pipeline.json",
    }
}
start = DummyOperator(task_id='start',dag=dag)
end = DummyOperator(task_id='end',dag=dag)
start_flex_template_job = DataflowStartFlexTemplateOperator(
    task_id="start_flex_template_job",
    project_id=project_id,
    body=BODY,
    location="us-central1",
    append_job_name=False,
    wait_until_finished=False,
    dag=dag
)
# Monitor the Dataflow job status
monitor_dataflow_job = DataflowJobSensor(
    task_id="monitor_dataflow_job",
    project_id=project_id,
    region="us-central1",
    job_id=f"food-orders-pipeline-{date}",
    mode='poke',  # Mode can be 'poke' or 'reschedule'
    timeout=600,  # Wait up to 10 minutes
    poke_interval=60,  # Check every 60 seconds,
    dag=dag
)

# Determine next steps based on job status
def branch_task(**kwargs):
    job_status = kwargs['task_instance'].xcom_pull(task_ids='monitor_dataflow_job')
    if job_status == 'JOB_STATE_DONE':
        return 'end'
    else:
        return 'log_error_to_bigquery'

branch = BranchPythonOperator(
    task_id='branch',
    python_callable=branch_task,
    provide_context=True,
    dag=dag
)

# Load data into BigQuery
log_error_to_bigquery = BigQueryInsertJobOperator(
    task_id="log_error_to_bigquery",
    configuration={
        "query": {
            "query": f"INSERT {bq_dataset}.{bq_error_table} VALUES ('job_id','error',CURRENT_DATE())",
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    location="us-central1",
    dag=dag
)

# Send failure email
send_failure_email = EmailOperator(
    task_id='send_failure_email',
    to='team@gmail.com',  # Replace with actual recipient(s)
    subject='Dataflow Job Failed',
    html_content='''
        <h3>Dataflow Job Failure Notification</h3>
        <p>The Dataflow job with ID {{ task_instance.xcom_pull(task_ids='monitor_dataflow_job') }} has failed.</p>
        <p>Please check the Dataflow logs for more details.</p>
    ''',
    dag=dag,
)

start_flex_template_job
start >> start_flex_template_job >> monitor_dataflow_job >> branch
branch >> end
branch >> log_error_to_bigquery >> end