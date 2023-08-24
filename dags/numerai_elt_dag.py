from datetime import timedelta
import pendulum
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, '/Users/jurisildam/numerai/numerai_elt_airflow/scripts')

#import config  # Now you should be able to import config

from config import DATA_TYPE


from gcp_utils import gcp_setup
from elt_task import main_elt_task


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
    schedule=timedelta(days=1),
    start_date=pendulum.today('UTC').add(days=-1),
    tags=['Numerai ELT'],
)

t1 = PythonOperator(
    task_id='gcp_setup_task',
    python_callable=gcp_setup,
    dag=dag,
)

t2 = PythonOperator(
    task_id='run_elt_task',
    python_callable=main_elt_task,
    dag=dag,
)

# Define the order in which the tasks should run
t1 >> t2