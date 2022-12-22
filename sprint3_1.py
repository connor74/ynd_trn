from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sql import (
    SQLCheckOperator,
    SQLValueCheckOperator,
)


# для таблицы user_order_log первая проверка
def check_success_insert_user_order_log(context):
    insert_dq_checks_results = PostgresOperator(
        task_id="success_insert_user_order_log",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_order_log', 'user_order_log_isNull', current_date, 0 )
          """
    )

def check_failure_insert_user_order_log(context):
    insert_dq_checks_results = PostgresOperator(
        task_id="failure_insert_user_order_log",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_order_log', 'user_order_log_isNull', current_date,  1)
          """
    )

# для таблицы user_activity_log первая проверка
def check_success_insert_user_activity_log(context):
    insert_dq_checks_results = PostgresOperator(
        task_id="success_insert_user_activity_log",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_activity_log', 'user_activity_log_isNull', current_date, 0 )
          """
    )
 
def check_failure_insert_user_activity_log(context):
    insert_dq_checks_results = PostgresOperator(
        task_id="failure_insert_user_activity_log",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_activity_log', 'user_activity_log_isNull', current_date,  1)
          """
    )

# для таблицы user_order_log вторая проверка
def check_success_insert_user_order_log2(context):
    insert_dq_checks_results = PostgresOperator(
        task_id="success_insert_user_order_log2",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_order_log', 'user_order_log_isNull', current_date, 0 )
          """
    )

def check_failure_insert_user_order_log2 (context):
    insert_dq_checks_results = PostgresOperator(
        task_id="failure_insert_user_order_log2",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_order_log', 'user_order_log_isNull', current_date,  1)
          """
    )

# для таблицы user_activity_log вторая проверка
def check_success_insert_user_activity_log2 (context):
    insert_dq_checks_results = PostgresOperator(
        task_id="success_insert_user_activity_log2",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_activity_log', 'user_activity_log_isNull', current_date, 0 )
          """
    )

def check_failure_insert_user_activity_log2 (context):
    insert_dq_checks_results = PostgresOperator(
        task_id="failure_insert_user_activity_log2",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_activity_log', 'user_activity_log_isNull', current_date,  1)
          """
    )

default_args = {
    "start_date": datetime(2020, 1, 1),
    "owner": "airflow",
    "conn_id": "postgres_default"
}



with DAG(
    dag_id="Sprin4_Task61", 
    schedule_interval="@daily", 
    default_args=default_args, 
    catchup=False
) as dag:

    begin = DummyOperator(task_id="begin")
    sql_check = SQLCheckOperator(
        task_id="user_order_log_isNull", 
        sql="user_order_log_isNull_check.sql", 
        on_success_callback = check_success_insert_user_order_log,
        on_failure_callback =  check_failure_insert_user_order_log
    )
    sql_check2 = SQLCheckOperator(
        task_id="user_activity_log_isNull", 
        sql="user_activity_log_isNull_check.sql", 
        on_success_callback = check_success_insert_user_activity_log,
        on_failure_callback =  check_failure_insert_user_activity_log
    )
    sql_check3 = SQLValueCheckOperator(
        task_id="check_row_count_user_order_log", 
        sql="Select count(distinct(customer_id)) from user_order_log", 
        pass_value=3, 
        tolerance=0.01,
        on_success_callback = check_success_insert_user_order_log2,
        on_failure_callback =  check_failure_insert_user_order_log2
    ) 
    sql_check4 = SQLValueCheckOperator(
        task_id="check_row_count_user_activity_log", 
        sql="Select count(distinct(customer_id)) from user_activity_log", 
        pass_value=3, 
        tolerance=0.01,
        on_success_callback = check_success_insert_user_activity_log2,
        on_failure_callback =  check_failure_insert_user_activity_log2
    ) 
    end = DummyOperator(task_id="end")

    begin >> [sql_check, sql_check2, sql_check3, sql_check4 ]>> end
    
    """
        Чтобы выполнить проверку № 1, нужно создать три сенсора, по одному для каждого из файлов, и объединить их в группу. Получившийся код надо добавить в описание ETL-процесса в Airflow. Затем обязательно нужно указать условие перехода к следующему шагу. Поскольку проверка № 1 критичная, переход возможен только если все три файла прошли проверку сенсорами.
    Затем — создать «Группу загрузки и проверки»: три шага с загрузкой данных из файлов в stage-слой объединить в группу. Объединить их нужно таким же способом, как и сенсоры при проектировании проверки №1.
    После загрузки данных из файлов user_order_log и user_activity_log в stage-слой добавить по два шага с проверками для двух этих таблиц — проверки № 2 и № 3.
    """
