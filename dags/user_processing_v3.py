# TaskFlow API syntax

from airflow.decorators import dag, task

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.sensors.http import HttpSensor

from pandas import json_normalize
from datetime import datetime

API: str = "https://randomuser.me/api"

# task 1: create users table in postgres
def create_users_table() -> PostgresOperator:
    create_table: PostgresOperator = PostgresOperator(
        task_id="create_table",
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS users (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL
            );
        '''
    )

    return create_table

# task 2: check if api is active
def check_api() -> HttpSensor:
    is_api_available: HttpSensor = HttpSensor(
        task_id = 'is_api_available',
        http_conn_id = 'user_api',
        endpoint = 'api/'
    )

    return is_api_available

# task 3: get api response
@task(task_id="extract_user")
def extract_user(api: str) -> dict:
    import requests

    response = requests.get(api)
    return response.json()

# task 4: process api response and save it to csv
@task(task_id="process_user")
def process_user(users: dict) -> None:
    user = users['results'][0]
    processed_user = json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email']
    })
    processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)

# task 5: save the csv info in users table in postgres via hook
@task(task_id="store_user")
def store_user() -> None:
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER AS ',' ",
        filename='/tmp/processed_user.csv'
    )

@dag(dag_id="user_processing_v3_dag", schedule="@daily", start_date=datetime(2023, 1, 1), catchup=False)
def user_processing_v3_dag():
    
    create_table = create_users_table()
    is_api_available = check_api()
    get_users = extract_user(api=API)
    wrangle_users = process_user(get_users)
    save_users = store_user()

    #depedencies
    create_table >> is_api_available >> get_users >> wrangle_users >> save_users

user_processing_v3_dag()