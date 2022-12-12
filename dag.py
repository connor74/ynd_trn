import datetime
import requests
import json
import time
import os
import psycopg2, psycopg2.extras
import pandas as pd


from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator



dag = DAG(
    dag_id='533_api_generate_report',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)
business_dt = {'dt':'2022-05-06'}

pg_conn_id = 'pg_connection'

API_KEY = "5f55e6c0-e9e5-4a9c-b313-63c01fc31460"
NICKNAME = "kurzanovart"
COHORT = "8"
URL_API = "https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/"

STAGE_DIR = "/lessons/stage/"

if not os.path.isdir(STAGE_DIR):
    os.mkdir(path)

headers = {
    "X-API-KEY": API_KEY,
    "X-Nickname": NICKNAME,
    "X-Cohort": COHORT
}

files = [
        "customer_research",
        "user_order_log",
        "user_activity_log",
    ]



def create_files_request(headers, **kwargs):
    ti = kwargs['ti']
    r = requests.post(
        f"{URL_API}/generate_report",
        headers=headers
    ).json()
    task_id = r["task_id"]
    print(task_id)    
    ti.xcom_push(value=task_id, key='task_id')

def generate_report(headers, **kwargs):
    ti = kwargs['ti']
    task_id = ti.xcom_pull(key="task_id")
    while True:
        get_report_response = requests.get(
            f"{URL_API}/get_report?task_id={task_id}",
            headers=headers
        ).json()
        if get_report_response['status'] == 'SUCCESS':
            report_id = get_report_response['data']['report_id']
            break
        else:
            time.sleep(10)
    print(report_id)
    ti.xcom_push(value=report_id, key='report_id')

def download_files(files, **kwargs):
    ti = kwargs['ti']
    #path = os.path.join(parent_dir, DIR_STAGE)
    if not os.path.isdir(STAGE_DIR):
        os.mkdir(STAGE_DIR)

    files = [
        "customer_research",
        "user_order_log",
        "user_activity_log",
    ]
    report_id = ti.xcom_pull(key="report_id")
    url = "https://storage.yandexcloud.net/s3-sprint3-static/lessons/"
    for file in files:
        #url = f"https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/{report_id}/{file}.csv"
        url = f"https://storage.yandexcloud.net/s3-sprint3-static/lessons/{file}.csv"
        res = requests.get(url)
        with open(STAGE_DIR+"/"+file+".csv", "wb") as file:
            file.write(res.content)

def load_file_to_pg(file, engine, schema):
    table = file
    df = pd.read_csv(f"{STAGE_DIR}{file}.csv" )

    postgres_hook = PostgresHook(pg_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table, engine, schema=schema, if_exists='append', index=False)




create_customer_research = PostgresOperator(
        task_id="create_customer_research",
        postgres_conn_id=pg_conn_id,
        sql="""
            DROP TABLE IF EXISTS stage.customer_research;
            CREATE TABLE stage.customer_research(
                ID                   serial ,
                date_id              TIMESTAMP ,
                category_id          INT ,
                geo_id               INT ,
                sales_qty            INT ,
                sales_amt            NUMERIC(14,2),
                PRIMARY KEY (ID)
            );     
        """,
    )

create_user_order_log= PostgresOperator(
        task_id="create_user_order_log",
        postgres_conn_id=pg_conn_id,
        sql="""
            DROP TABLE IF EXISTS stage.user_order_log; 
            CREATE TABLE stage.user_order_log (
                id                 serial,
                date_time          TIMESTAMP,
                city_id            INT,
                city_name          VARCHAR(100),
                customer_id        BIGINT,
                first_name         VARCHAR(100),
                last_name          VARCHAR(100),
                item_id            INT,
                item_name          VARCHAR(100),
                quantity           BIGINT,
                payment_amount     NUMERIC(14,2),
                PRIMARY KEY (id)
            );     
        """,
    )

create_files_request = PythonOperator(task_id='create_files_request',
                                        python_callable=create_files_request,
                                        op_kwargs={"headers": headers},
                                        dag=dag)
generate_files = PythonOperator(task_id='generate_files',
                                        python_callable=generate_report,
                                        op_kwargs={"headers": headers},
                                        dag=dag)
download_files = PythonOperator(task_id='download_files',
                                        python_callable=download_files,
                                        op_kwargs={"files": files},
                                        dag=dag) 
to_sql_customer_research = PythonOperator(task_id='to_sql_customer_research',
                                        python_callable=load_file_to_pg,
                                        op_kwargs={
                                            "file": files[0],
                                            "engine": "de",
                                            "schema": "stage",                                            
                                            },
                                        dag=dag)  
to_sql_user_order_log = PythonOperator(task_id='to_sql_user_order_log',
                                        python_callable=load_file_to_pg,
                                        op_kwargs={
                                            "file": files[1],
                                            "engine": "de",
                                            "schema": "stage",                                            
                                            },
                                        dag=dag)                                  

create_files_request >> generate_files >> download_files >> [create_customer_research, create_user_order_log] >> to_sql_customer_research >> to_sql_user_order_log
