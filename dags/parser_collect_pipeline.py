from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import tqdm
import logging

def init_db():
    """Инициализация подключения к базе данных"""
    try:
        engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')
        logging.info("Подсоединение к базе данных прошло удачно")
        
        with engine.connect() as conn:
            # Выполняем запрос и получаем результат
            result = conn.execute(text("SELECT 'HELLO, AIRFLOW!' as greeting"))
            
            # Извлекаем и логируем результат
            row = result.fetchone()
            if row:
                greeting = row[0]
                logging.info(f"Результат запроса: {greeting}")
            else:
                logging.info("Запрос не вернул результатов")
            
        return "database_connection_success"
        
    except Exception as e:
        logging.error(f"Ошибка подключения к базе данных: {e}")
        raise

with DAG(
    'parcer_collect_data',
    description='Парсер данных',
    start_date = datetime(2025, 10, 17),
    schedule_interval=None,
    catchup=False,
) as dag:

    init_db_task = PythonOperator(
        task_id='inint_db',
        python_callable=init_db,
    )