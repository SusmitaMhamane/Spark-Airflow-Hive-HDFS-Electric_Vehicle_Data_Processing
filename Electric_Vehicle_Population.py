from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow import DAG
from datetime import timedelta
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator

import subprocess

def execute_bash_script():
    bash_command = ['/home/talentum/Desktop/airflow-tutorial/dags/scripts/Electric_Vehicle.sh']
    result = subprocess.run(bash_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        raise Exception(f"Bash script failed with error: {result.stderr}")
    else:
        print("Bash script executed successfully")


default_args = {
    'owner': 'Sushmita',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG('Electric_vehicle', default_args=default_args, schedule_interval=None) as dag:
    upload_file = PythonOperator(
        task_id='upload_file',
        python_callable=execute_bash_script,
    )

    # Task for data transformation using Spark
    data_transform = SparkSubmitOperator(
        task_id='data_transform',
        application='/home/talentum/test-jupyter/P2/selfEvalLab/airflowLab/electricVehicle.py',
        conn_id='spark_default',
        env_vars={
            'spark.executorEnv.PYSPARK_DRIVER_PYTHON': '/usr/bin/python3.6',
            'spark.executorEnv.PYSPARK_DRIVER_PYTHON_OPTS': '',
            'spark.executorEnv.PYSPARK_PYTHON': '/usr/bin/python3.6',
            'spark.executorEnv.PYTHONPATH': '/usr/lib/python3.6',
        }
    )

    # Task for data querying using Hive
    data_query = BashOperator(
        task_id='data_query',
        bash_command='hive -e "SELECT * FROM electric.electricVehicles LIMIT 10;"'
    )

    # Set dependencies
    upload_file >> data_transform >> data_query
