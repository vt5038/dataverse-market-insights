from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import os
import subprocess

# -----------------------------
# DAG Configuration
# -----------------------------
default_args = {
    'owner': 'jayanth',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    dag_id='dataverse_market_insights_etl',
    description='ETL Pipeline: CoinGecko → S3 → Glue → Athena',
    default_args=default_args,
    schedule_interval='0 */6 * * *',   # ⏰ every 6 hours
    start_date=days_ago(1),
    catchup=False,
    tags=['dataverse', 'crypto', 'etl']
)

# -----------------------------
# Helper Function to run scripts
# -----------------------------
def run_script(script_name):
    """Runs a Python script located in /opt/airflow/scripts/ inside the container."""
    script_path = f"/opt/airflow/scripts/{script_name}"
    print(f"🚀 Running script: {script_path}")
    result = subprocess.run(['python', script_path], capture_output=True, text=True)

    if result.returncode != 0:
        print(f"❌ Script failed: {result.stderr}")
        raise Exception(f"Script {script_name} failed with error:\n{result.stderr}")
    
    print(f"✅ Script {script_name} completed successfully.")
    print(result.stdout)

# -----------------------------
# Airflow Tasks
# -----------------------------
extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=run_script,
    op_args=['extract_coingecko.py'],
    dag=dag
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=run_script,
    op_args=['transform_coingecko.py'],
    dag=dag
)

create_glue_table = PythonOperator(
    task_id='create_glue_table',
    python_callable=run_script,
    op_args=['create_glue_table.py'],
    dag=dag
)

query_with_athena = PythonOperator(
    task_id='query_with_athena',
    python_callable=run_script,
    op_args=['query_athena.py'],
    dag=dag
)

def notify_completion():
    print(f"✅ DAG completed successfully — CoinGecko ETL pipeline finished at {datetime.utcnow()} UTC")

notify_completion_task = PythonOperator(
    task_id='notify_completion',
    python_callable=notify_completion,
    dag=dag
)

# -----------------------------
# Task Dependencies
# -----------------------------
extract_data >> transform_data >> create_glue_table >> query_with_athena >> notify_completion_task
