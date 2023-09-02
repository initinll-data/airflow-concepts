from airflow.decorators import dag, task_group
from airflow.operators.bash import BashOperator
 
from datetime import datetime

@task_group(group_id='download_tasks_group')
def download_tasks():
    download_a = BashOperator(
        task_id='download_a',
        bash_command='sleep 10'
    )
 
    download_b = BashOperator(
        task_id='download_b',
        bash_command='sleep 10'
    )
 
    download_c = BashOperator(
        task_id='download_c',
        bash_command='sleep 10'
    )

@task_group(group_id='transform_tasks_group')
def transform_tasks():
    transform_a = BashOperator(
        task_id='transform_a',
        bash_command='sleep 10'
    )
 
    transform_b = BashOperator(
        task_id='transform_b',
        bash_command='sleep 10'
    )

    transform_c = BashOperator(
        task_id='transform_c',
        bash_command='sleep 10'
    )

@dag(dag_id='group_dag_v2', start_date=datetime(2023, 1, 1),schedule='@daily', catchup=False)
def group_dag_v2():

    download_files = download_tasks()
 
    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )

    transform_files = transform_tasks()
 
    download_files >> check_files >> transform_files

group_dag_v2()