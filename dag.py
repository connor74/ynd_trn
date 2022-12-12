from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

import datetime
import requests
import json
import time
import os
import psycopg2, psycopg2.extras

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

def download_files(**kwargs):
    ti = kwargs['ti']
    path = os.path.join(parent_dir, DIR_STAGE)
    if not os.path.isdir(path):
        os.mkdir(path)

    files = [
        "customer_research.csv",
        "user_order_log.csv",
        "user_activity_log.csv",
    ]
    report_id = ti.xcom_pull(key="report_id")
    url = "https://storage.yandexcloud.net/s3-sprint3-static/lessons/"
    for file in files:
        #url = f"https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/{report_id}/{file}"
        url = f"https://storage.yandexcloud.net/s3-sprint3-static/lessons/{file}"
        res = requests.get(url)
        print(path+file)
        with open(path+"/"+file, "wb") as file:
            file.write(res.content)

def load_file_to_pg(file):

    df = pd.read_csv(f"{STAGE_DIR}{file}.csv" )

    cols = ','.join(list(df.columns))
    insert_stmt = f"INSERT INTO stage.{file} ({*ваш код здесь*}) VALUES %s"

    pg_conn = psycopg2.connect(*ваш код здесь*)
    cur = pg_conn.cursor()

    psycopg2.extras.execute_values(cur, insert_stmt, df.values)
    pg_conn.commit()

    cur.close()
    pg_conn.close() 


create_customer_research = PostgresOperator(
        task_id="create_customer_research",
        postgres_conn_id="pg_connection",
        sql="""
            CREATE TABLE IF NOT EXISTS stage.customer_research (
                date_id datetime,
                category_id int,
                geo_id int,
                sales_qty bigint,
                sales_amt decimal(10, 2));
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
                                        dag=dag)                                    

create_files_request >> generate_files >> download_files >> create_customer_research
