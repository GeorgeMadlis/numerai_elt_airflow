from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


from gcp_utils import gcp_setup
from bigquery_pipeline import run_elt

# Airflow DAG setup
data_type = DATA_TYPE

# Airflow DAG definition
default_args = {
    'owner': 'me',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'elt_dag',
    default_args=default_args,
    description='A Numerai ELT DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['Numerai ELT'],
)

t1 = PythonOperator(
    task_id='gcp_setup_task',
    python_callable=gcp_setup,
    dag=dag,
)

t2 = PythonOperator(
    task_id='run_elt_task',
    python_callable=run_elt,
    dag=dag,
)

# Define the order in which the tasks should run
t1 >> t2