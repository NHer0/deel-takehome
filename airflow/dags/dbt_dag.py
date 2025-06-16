from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dbt_models',
    default_args=default_args,
    description='Run dbt models',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Define the dbt commands
dbt_debug = BashOperator(
    task_id='dbt_debug',
    bash_command='cd /opt/airflow/dbt/deel_takehome && dbt debug',
    dag=dag,
)

dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command='cd /opt/airflow/dbt/deel_takehome && dbt deps',
    dag=dag,
)

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dbt/deel_takehome && dbt run',
    dag=dag,
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/airflow/dbt/deel_takehome && dbt test',
    dag=dag,
)

# Set task dependencies
dbt_debug >> dbt_deps >> dbt_run >> dbt_test 