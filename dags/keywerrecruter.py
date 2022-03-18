from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import csv
from airflow.hooks.base_hook import BaseHook
import json
from pandas import json_normalize
import sys, psycopg2

default_args = {
    'start_date': datetime(2022, 3, 16)

}

def _processing_user(ti):
    #do 

    users = ti.xcom_pull(task_ids=['extracting_user'])
    if not len(users) or 'results' not in users[0]:
        raise ValueError('User is empty')
    user = users[0]['results'][0]
    proccesed_user= json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['first'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email']
    })
    proccesed_user.to_csv('/tmp/processed_user.csv',index=None,header=False)

def _store_data(ti):
  
    print('Hi from python operator')
    connection = BaseHook.get_connection("postgres")
    # Using connection to: id: postgres. Host: postgres, Port: 5432, Schema: ***, Login: ***, Password: ***, extra: {}
    print(connection)
    conn = psycopg2.connect(database="airflow", user="airflow",password="airflow", host="postgres")
    cur = conn.cursor()
    with open('/tmp/processed_user.csv', 'r') as f:
        reader = csv.reader(f)
        # next(reader) # Skip the header row.
        for row in reader:
            cur.execute(
            "INSERT INTO users VALUES (%s, %s, %s, %s,%s, %s)",
            row
        )
    conn.commit()
    

with DAG('keywer_recruter', default_args=default_args, description='A simple tutorial DAG',schedule_interval='@daily', start_date=datetime(2022, 3, 15),tags=['example'],    catchup=True) as dag:
    

    creating_table = PostgresOperator(
        task_id="creating_table",
        postgres_conn_id="postgres",
        sql="""
            Create Table IF NOT EXISTS Users (
                   firstname TEXT NOT NULL,
                   lastname  TEXT NOT NULL,
                   country  TEXT NOT NULL,
                   username  TEXT NOT NULL,
                   password  TEXT NOT NULL,
                   email  TEXT NOT NULL PRIMARY KEY
                ); 
          """,
    )

    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/'
    )

    extracting_user = SimpleHttpOperator(
        task_id='extracting_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    processing_user  = PythonOperator(
        task_id="processing_user",
        python_callable=_processing_user,
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )
    store_user  = PythonOperator(
        task_id="store_user",
        python_callable=_store_data,
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )
   
    
    creating_table >> is_api_available >> extracting_user >> processing_user >> store_user