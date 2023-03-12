from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, \
    DataprocSubmitPySparkJobOperator, \
    DataprocDeleteClusterOperator
import airflow

SPARK_BUCKET = 'gs://spark-etl-files'
PROJECT_ID = ''
DATAPROC_NAME = 'dataproc-transfer'
REGION = ''

default_args = {
    'depends_on_past': False,
    'retries': 1,
    'start_date': airflow.utils.dates.days_ago(0),
    'retry_delay': timedelta(minutes=5),
}

CONFIG = {
    "initialization_actions": [
        {
            "executable_file": SPARK_BUCKET+"/dataproc-init.sh"
        }
    ],
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {
            "boot_disk_size_gb": 500,
            "num_local_ssds": 0
        }
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {
            "boot_disk_size_gb": 500,
            "num_local_ssds": 0
        }
    }
}

dag = DAG('start_dataproc_cluster',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False
          )

create_dataproc_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    cluster_name=DATAPROC_NAME,
    project_id=PROJECT_ID,
    region=REGION,
    cluster_config=CONFIG,
    dag=dag
)

run_pyspark_job = DataprocSubmitPySparkJobOperator(
    task_id='dataproc_pyspark_task',
    main=SPARK_BUCKET+'/spark-etl.py',
    arguments=['test_table.json'],
    cluster_name=DATAPROC_NAME,
    region=REGION,
    dataproc_properties={'spark.driver.extraClassPath': SPARK_BUCKET+'/postgresql-42.5.1.jar'},
    dataproc_jars=[SPARK_BUCKET+'/postgresql-42.5.1.jar',
                   SPARK_BUCKET+'/spark-3.1-bigquery-0.28.0-preview.jar'],
    dag=dag
)

delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_dataproc_cluster',
    project_id=PROJECT_ID,
    cluster_name=DATAPROC_NAME,
    region=REGION,
    dag=dag
)

create_dataproc_cluster >> run_pyspark_job >> delete_cluster