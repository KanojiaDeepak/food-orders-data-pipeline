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
    'daily_dataflow_job1',
    default_args=default_args,
    description='Run Dataflow job daily',
    schedule_interval='0 0 * * *',  # Run at midnight UTC every day
    catchup=False,  # Skip catching up on historical DAG runs
)

#date=datetime.now().strftime('%d_%m_%Y')
date='12_07_2024'

# BODY = {
#     "launchParameter": {
#         "jobName": f"food-orders-pipeline",
#         "parameters": {
#             "input": f"gs://food_orders_delivery/data/food_daily_{date}.csv",
#             "dataset": f"demodataset",
#             "staging_location":"gs://food_orders_delivery/tmp/"
#         },
#         "environment": {},
#         "containerSpecGcsPath": "gs://food_orders_delivery/batch_pipeline.json",
#     }
# }

# start_flex_template_job = DataflowStartFlexTemplateOperator(
#     task_id="start_flex_template_job",
#     project_id=project_id,
#     body=BODY,
#     location='us-central1',
#     append_job_name=True,
#     wait_until_finished=True,
#     dag=dag
# )

def get_job_id(**kwargs):
    # The job_id will be passed to XCom by the DataflowCreatePythonJobOperator
    job_id = kwargs['task_instance'].xcom_pull(task_ids='dataflow_job')
    return job_id

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
        wait_until_finished=False
    )
extract_job_id = PythonOperator(
        task_id='extract_job_id',
        python_callable=get_job_id,
        provide_context=True,
        dag=dag
    )

monitor_job_status = DataflowJobStatusSensor(
        task_id='monitor_job_status',
        job_id="{{ task_instance.xcom_pull(task_ids='extract_job_id') }}",
        expected_statuses=['JOB_STATE_DONE','JOB_STATUS_FAILED'],  # Or other status you want to monitor
        mode='reschedule',  # 'poke' is also an option
        timeout=600,  # 10 minutes timeout
        poke_interval=60,  # Check every 60 seconds
        dag=dag
    )

start = DummyOperator(task_id='start',dag=dag)
end = DummyOperator(task_id='end',dag=dag)

start >> dataflow_job >> extract_job_id >> monitor_job_status >> end