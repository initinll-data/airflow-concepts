from airflow import Dataset
from airflow.decorators import dag, task

from datetime import datetime

MY_FILE_1: Dataset = Dataset("/tmp/my_file_1.txt")
MY_FILE_2: Dataset = Dataset("/tmp/my_file_2.txt")

@task(task_id="consume_datasets")
def consume_datasets() -> None:
    with open(MY_FILE_1.uri, "r") as file_1:
        print(file_1.read())
    with open(MY_FILE_2.uri, "r") as file_2:
        print(file_2.read())


@dag(dag_id="dataset_consumer_dag", 
     schedule=[MY_FILE_1, MY_FILE_2], 
     start_date=datetime(2023, 1, 1), 
     catchup=False)
def dataset_consumer_dag():
    consume_datasets()

dataset_consumer_dag()
