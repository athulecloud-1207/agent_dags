from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
import os
import logging

# Set up logging
logger = logging.getLogger("airflow.task")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

def check_and_process_files(**context):
    base_path = '/appz/data/vector_watch_file_pdf'
    
    try:
        # Check if base folder exists
        if not os.path.exists(base_path):
            logger.error(f"Base folder {base_path} does not exist")
            raise Exception(f"Base folder {base_path} not found")
        
        # Get all UUID directories
        uuid_dirs = [d for d in os.listdir(base_path) 
                    if os.path.isdir(os.path.join(base_path, d))]
        
        if not uuid_dirs:
            logger.info("No UUID directories found in the monitored folder")
            return  # Stop execution if no UUID dirs
        
        pdf_found = False
        for uuid_dir in uuid_dirs:
            uuid_path = os.path.join(base_path, uuid_dir)
            
            # Walk through directory tree
            for root, dirs, files in os.walk(uuid_path):
                # Skip archive folder
                if 'archive' in dirs:
                    dirs.remove('archive')
                
                # Check for PDF files
                pdf_files = [f for f in files if f.lower().endswith('.pdf')]
                
                if pdf_files:
                    pdf_found = True
                    logger.info(f"PDF files found in {root}: {pdf_files}")
                    
                    # Trigger the processing DAG for this UUID
                    context['ti'].xcom_push(key=f'uuid_{uuid_dir}', value=uuid_dir)
                    return  # Return after first PDF found to process one at a time
        
        if not pdf_found:
            logger.info("No PDF files found in any UUID directories")
            return  # Stop execution if no PDFs found
            
    except Exception as e:
        logger.error(f"Error in check_and_process_files: {str(e)}")
        raise

# DAG definition
try:
    with DAG(
        'shared_monitor_folder_pdf',
        default_args=default_args,
        description='Monitors UUID folders for PDF files and triggers processing',
        schedule_interval='* * * * *',  # Runs every minute
        start_date=days_ago(1),
        catchup=False,
    ) as dag:

        # Start task
        start = DummyOperator(
            task_id='start'
        )

        # Check folder task
        check_folder = PythonOperator(
            task_id='check_pdf_folder',
            python_callable=check_and_process_files,
            provide_context=True,
        )

        # Trigger processing DAG
        trigger_processing = TriggerDagRunOperator(
            task_id='trigger_pdf_processing',
            trigger_dag_id='shared_process_file_pdf2vector',
            conf={"uuid": "{{ ti.xcom_pull(task_ids='check_pdf_folder', key='uuid_*') }}"},
            execution_date="{{ ds }}",
            reset_dag_run=True,
            wait_for_completion=False,
            trigger_rule='all_success'  # Only trigger if check_folder succeeds
        )

        # End task
        end = DummyOperator(
            task_id='end'
        )

        # Task dependencies
        start >> check_folder >> trigger_processing >> end

except Exception as e:
    logger.error(f"Failed to initialize DAG shared_monitor_folder_pdf: {str(e)}")
    raise

logger.info("DAG shared_monitor_folder_pdf loaded successfully")