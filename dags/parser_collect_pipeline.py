from sqlalchemy import create_engine
import logging
from datetime import datetime


from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator


with DAG(
    dag_id='parcer_collect_data',
    description='ДАГ, который активирует парсер и собирает данные с сайта Ireccomend',
    start_date = datetime(2025, 10, 10)
) as dag:


    def get_engine():
        engine = create_engine('postgresql+psycopg2://airflow:airflow@localhost:5432/')
        logging.info('Подсоединение к базе данных прошло удачно')

        return engine

    def print_hello():
        try:
            conn = get_engine().connect().execute('select "1"')
            print(conn)
        except Exception as e:
            print('ABC, всем приает')


    start_task = PythonOperator(task_id = 'inint_db', python_callable = get_engine)
    hello_task = PythonOperator(task_id = '123', python_callable = print_hello)


    start_task >> hello_task