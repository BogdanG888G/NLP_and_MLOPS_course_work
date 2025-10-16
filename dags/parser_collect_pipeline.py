from sqlalchemy import create_engine
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.operators.

def get_engine():
    engine = create_engine('postgresql+psycopg2://Bogdan:123@localhost:5432/Test')
    logging.info('Подсоединение к базе данных прошло удачно')

    return engine


with DAG(
    dag_id='parcer_collect',
    description='ДАГ, который активирует парсер и собирает данные с сайта Ireccomend',
    schedule='@daily',
    max_active_runs = 1,
    max_active_tasks=2
)