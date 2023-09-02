from airflow import Dataset
from airflow.decorators import dag, task

from datetime import datetime

MY_FILE_1: Dataset = Dataset("/tmp/my_file_1.txt")
MY_FILE_2: Dataset = Dataset("/tmp/my_file_2.txt")

@task(task_id="update_dataset_1", outlets=[MY_FILE_1])
def update_dataset_1() -> None:
    with open(MY_FILE_1.uri, "a+") as file:
        file.write("producer update 1")

@task(task_id="update_dataset_2", outlets=[MY_FILE_2])
def update_dataset_2() -> None:
    with open(MY_FILE_2.uri, "a+") as file:
        file.write("producer update 2")

@dag(dag_id="dataset_producer_dag", schedule="@daily", start_date=datetime(2023, 1, 1), catchup=False)
def dataset_producer_dag():
    update_dataset_1() >> update_dataset_2()

dataset_producer_dag()
