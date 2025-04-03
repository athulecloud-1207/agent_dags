from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 3),  # Replace with your start date
}

# Define the DAG
dag = DAG(
    'dbt_run_and_seed',
    default_args=default_args,
    description='DAG for running dbt run and dbt seed',
    schedule_interval=None,  # You can adjust this to run on a schedule, e.g., '0 0 * * *' for daily
)

# Task to run `dbt seed`
dbt_seed = BashOperator(
    task_id='dbt_seed',
    bash_command='dbt seed --profiles-dir /path/to/profiles',
    dag=dag,
)

# Task to run `dbt run`
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='dbt run --profiles-dir /path/to/profiles',
    dag=dag,
)

# Set the order of execution (dbt seed should run before dbt run)
dbt_seed >> dbt_run
