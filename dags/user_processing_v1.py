# traditional syntax

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

import json
from pandas import json_normalize
from datetime import datetime

def _process_user(ti): 
    user = ti.xcom_pull(task_ids='extract_user')
    user = user['results'][0]
    processed_user = json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email']
    })
    processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)

def _store_user():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER AS ',' ",
        filename='/tmp/processed_user.csv'
    )


with DAG('user_processing_v1_dag', start_date=datetime(2023, 1, 1),
        schedule='@daily', catchup=False) as dag:

    # task 1: create users table in postgres
    create_table = PostgresOperator(
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

    # task 2: check if api is active
    is_api_available = HttpSensor(
        task_id = 'is_api_available',
        http_conn_id = 'user_api',
        endpoint = 'api/'
    )

    # task 3: get api response
    extract_user = SimpleHttpOperator(
        task_id = 'extract_user',
        http_conn_id='user_api',
        endpoint = 'api/',
        method='GET',
        response_filter= lambda respose: json.loads(respose.text),
        log_response=True)
    
    # task 4: process api response and save it to csv
    process_user = PythonOperator(
        task_id = 'process_user',
        python_callable=_process_user)
    
    # task 5: save the csv info in users table in postgres via hook
    store_user = PythonOperator(
        task_id = 'store_user',
        python_callable=_store_user
    )
    
    #depedencies
    create_table >> is_api_available >> extract_user >> process_user >> store_user