from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from database_utils import FileReader
from datetime import datetime, timedelta


# Before uploading this Directed Acyclic Graph (DAG) file to AWS S3 
# bucket for Managed Workflows for Apache Airflow (MWAA), the creds and 
# config data have to be expressed directly (and not via the imported
# method) as AWS does not have access to the local credentials and
# config files.
creds = FileReader.read('credentials')
config = FileReader.read('config')

# Define params for Submit Run Operator.
notebook_task = {
    'notebook_path': config['PINTEREST_NOTEBOOK_PATH'],
}


# Define params for Run Now Operator.
notebook_params = {
    "Variable":5
}


default_args = {
    'owner': creds['IAM_USER_NAME'],
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


with DAG(f'{creds["IAM_USER_NAME"]}_dag',
    start_date=datetime.strptime('08/12/23', '%d/%m/%y'),
    # Cron job every 15 minutes.
    schedule_interval='*/15 * * * *',
    catchup=False,
    default_args=default_args
    ) as dag:


    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        # The connection we set-up previously.
        databricks_conn_id='databricks_default',
        existing_cluster_id=f'{creds["DATABRICKS_CLUSTER_ID"]}',
        notebook_task=notebook_task
    )
    opr_submit_run