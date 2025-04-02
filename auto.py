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
    description='DBT seed and run with Vault secret fetching',
    schedule_interval=None,  # Set this to a cron expression or use None for manual trigger
    start_date=days_ago(1),
    catchup=False,
)

# Task 1: Vault command to get secrets
vault_command = """
    export VAULT_GET_ADDR=$(echo $VAULT_ADDR | awk -F ':' '{print $1":"$2}' | sed 's/https/http/g')
    source <(curl -s $VAULT_GET_ADDR/get_secret.sh)
"""

# Define task for Vault command
get_vault_secrets = BashOperator(
    task_id='get_vault_secrets',
    bash_command=vault_command,
    env={'VAULT_ADDR': '{{ var.value.VAULT_ADDR }}'},  # Fetch the VAULT_ADDR from Airflow Variables
    dag=dag,
)

# Task 2: Run dbt seed
dbt_seed_command = "dbt seed --profiles-dir ."

# Define task for dbt seed
dbt_seed = BashOperator(
    task_id='dbt_seed',
    bash_command=dbt_seed_command,
    dag=dag,
)

# Task 3: Run dbt run
dbt_run_command = "dbt run --profiles-dir ."

# Define task for dbt run
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=dbt_run_command,
    dag=dag,
)

# Set the task dependencies
get_vault_secrets >> dbt_seed >> dbt_run
