from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'dbt_webshop_pipeline',
    default_args=default_args,
    description='A DAG to run dbt seed and run commands for webshop project',
    schedule_interval=timedelta(days=1),  # Adjust schedule as needed
    start_date=datetime(2025, 4, 3),
    catchup=False,
) as dag:

    # Task 1: Run dbt seed
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command="""
        cd /appz/home/airflow/dags/agent_dags/dbt/webshop &&
        source /dbt_venv/bin/activate &&
        dbt seed --target dev
        """,
        dag=dag,
    )

    # Task 2: Run dbt run
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command="""
        cd /appz/home/airflow/dags/agent_dags/dbt/webshop &&
        source /dbt_venv/bin/activate &&
        dbt run --target dev
        """,
        dag=dag,
    )

    # Set task dependencies
    dbt_seed >> dbt_run
