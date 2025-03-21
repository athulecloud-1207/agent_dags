from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import requests
from pathlib import Path
import uuid
from datetime import timedelta
import logging

# Configure logging
logger = logging.getLogger("airflow.task")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'shared_process_file_pdf2vector',
    default_args=default_args,
    description='Upload PDFs to vector API with folder-based tags',
    start_date=days_ago(1),
    catchup=False,
)

def upload_pdfs(**kwargs):
    """
    Function to scan directory and upload PDFs with appropriate tags
    Includes UUID validation and file presence check
    """
    base_path = "/appz/data/vector_watch_file_pdf/"
    api_endpoint = "http://vector:8000/vector/pdf"
    
    # Get all immediate subdirectories
    subdirs = [d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))]
    
    if not subdirs:
        logger.info("No subdirectories found in base path")
        
    
    found_valid_files = False
    
    for subdir in subdirs:
        full_path = os.path.join(base_path, subdir)
        
        # Validate UUID
        try:
            uuid.UUID(subdir)
        except ValueError:
            logger.info(f"Skipping directory {subdir} - not a valid UUID")
            continue
            
        # Check for PDF files
        pdf_files = [f for f in os.listdir(full_path) if f.endswith('.pdf')]
        
        if not pdf_files:
            logger.info(f"No PDF files found in UUID directory: {subdir}")
            continue
        logger.info(f"PDF files found in UUID directory: {pdf_files}")
        found_valid_files = True
        logger.info(f"PDF files found in UUID directory: {subdir}")
        
        # Extract tags from path (if any subdirectories exist)
        path_parts = Path(full_path).relative_to(base_path).parts
        tags = list(path_parts[1:]) if len(path_parts) > 1 else []
        
        # Process each PDF file
        for pdf_file in pdf_files:
            file_path = os.path.join(full_path, pdf_file)
            
            # Prepare the upload data
            files = {'file': (pdf_file, open(file_path, 'rb'), 'application/pdf')}
            data = {'tags': ','.join(tags)} if tags else {}
            
            try:
                # Make the API call
                response = requests.post(api_endpoint, files=files, data=data)
                response.raise_for_status()
                
                logger.info(f"Successfully uploaded {pdf_file} with tags: {tags}")
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error uploading {pdf_file}: {str(e)}")
                
            finally:
                # Close the file
                files['file'][1].close()
    
    if not found_valid_files:
        logger.info("No valid PDF files found in any UUID directory - stopping DAG")
        return

# Define the task
upload_task = PythonOperator(
    task_id='upload_pdfs_to_vector',
    python_callable=upload_pdfs,
    dag=dag,
)

# Set task dependencies (if any)
upload_task