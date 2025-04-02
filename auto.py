from airflow import DAG
from airflow.providers.bash.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'dbt_vault_automation',
    default_args=default_args,
    description='DBT seed and run automation',
    schedule_interval=None,  # Set this to a cron expression or use None for manual trigger
    start_date=days_ago(1),
    catchup=False,
)

# Task 1: Run dbt seed
dbt_seed_command = "dbt seed --profiles-dir ."

# Define task for dbt seed
dbt_seed = BashOperator(
    task_id='dbt_seed',
    bash_command=dbt_seed_command,
    dag=dag,
)

# Task 2: Run dbt run
dbt_run_command = "dbt run --profiles-dir ."

# Define task for dbt run
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=dbt_run_command,
    dag=dag,
)

# Set the task dependencies
dbt_seed >> dbt_run
