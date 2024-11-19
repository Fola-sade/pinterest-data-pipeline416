# Databricks notebook source
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

# Define params for the first notebook
notebook_task_1 = {
    'notebook_path': '/Users/zzzzzzzzzz@gmail.com/Data Cleaning the dataframes',
}

# Define params for the second notebook
notebook_task_2 = {
    'notebook_path': '/Users/zzzzzzzzzz@gmail.com/Queries',
}

#Define params for Run Now Operator
notebook_params = {
    "Variable":5
}

# Define default arguments for the DAG
default_args = {
    'owner': 'Fola Falodun',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

# Define the DAG
with DAG(
    '<user_id>_dag',
    start_date=datetime(2024, 11, 16),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
) as dag:

    # Task 1: Submit the first notebook
    opr_submit_run_1 = DatabricksSubmitRunOperator(
        task_id='submit_run_1',
        databricks_conn_id='databricks_default',  # Connection ID for Databricks
        existing_cluster_id='1108-zzzzzz-zzzzzzzzzz',  # Replace with your cluster ID
        notebook_task=notebook_task_1,
    )

    # Task 2: Submit the second notebook
    opr_submit_run_2 = DatabricksSubmitRunOperator(
        task_id='submit_run_2',
        databricks_conn_id='databricks_default',  # Connection ID for Databricks
        existing_cluster_id='1108-zzzzzz-zzzzzzzzzz',  # Replace with your cluster ID
        notebook_task=notebook_task_2,
    )

    # Define task dependencies (Task 2 runs after Task 1)
    opr_submit_run_1 >> opr_submit_run_2

