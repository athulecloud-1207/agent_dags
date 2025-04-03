import os
import subprocess
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Path to the dbt virtual environment and the project directory
DBT_VENV_PATH = '/dbt_venv/bin/activate'
DBT_PROJECT_PATH = '/appz/home/airflow/dags/agent_dags/dbt/webshop'

# Function to execute a shell command
def run_dbt_command(command: str):
    # Set the command to activate the virtual environment and run the dbt command
    activate_venv = f"source {DBT_VENV_PATH} && cd {DBT_PROJECT_PATH} && {command}"
    
    # Run the command
    result = subprocess.run(activate_venv, shell=True, text=True, capture_output=True)
    
    # Check if the command was successful
    if result.returncode != 0:
        raise Exception(f"Error running dbt command: {result.stderr}")
    return result.stdout

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 3),  # Adjust start date as necessary
    'retries': 1,
}

dag = DAG(
    'dbt_webshop_dag',
    default_args=default_args,
    description='DAG to run dbt seed and dbt run commands',
    schedule_interval=None,  # Set to `None` for manual triggering or adjust for a schedule
    catchup=False,
)

# Define the tasks
def run_dbt_seed():
    return run_dbt_command('dbt seed')

def run_dbt_run():
    return run_dbt_command('dbt run')

# Task for running dbt seed
task_seed = PythonOperator(
    task_id='dbt_seed',
    python_callable=run_dbt_seed,
    dag=dag,
)

# Task for running dbt run
task_run = PythonOperator(
    task_id='dbt_run',
    python_callable=run_dbt_run,
    dag=dag,
)

# Set the task dependencies
task_seed >> task_run  # Ensure dbt run runs after dbt seed
