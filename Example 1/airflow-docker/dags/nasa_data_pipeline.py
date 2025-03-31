from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define default arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 28),
    "retries": 0,
}

# Define the DAG
dag = DAG(
    "nasa_data_pipeline",
    default_args=default_args,
    schedule_interval=None,  # Run manually for now
    catchup=False,
)

# Task 1: Retrieve NASA data
fetch_nasa_data = BashOperator(
    task_id="fetch_nasa_data",
    bash_command="python /opt/airflow/scripts/fetch_data.py",
    dag=dag,
)

# Task 2: Load data into PostgreSQL
load_to_postgres = BashOperator(
    task_id="load_to_postgres",
    bash_command="python /opt/airflow/scripts/load_to_postgres.py",
    dag=dag,
)

# Set task dependencies
fetch_nasa_data >> load_to_postgres
