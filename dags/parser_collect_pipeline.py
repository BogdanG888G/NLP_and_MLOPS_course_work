from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import psycopg2
import logging
import pandas as pd
import os
import sys
import importlib.util

# Базовые настройки
BASE_URL = "https://irecommend.ru"
START_URL = "https://irecommend.ru/catalog/reviews/939-13393"  # Добавьте эту переменную
TOTAL_PAGES = 2

# Настройка логирования
logging.basicConfig(level=logging.INFO)

def load_parser_module():
    """Динамическая загрузка модуля парсера"""
    try:
        # Получаем абсолютный путь к директории DAG
        parser_path = "/opt/airflow/parsers/irecommend_parser.py"
        
        logging.info(f"🔍 Ищем парсер по пути: {parser_path}")
        
        if not os.path.exists(parser_path):
            raise FileNotFoundError(f"Файл парсера не найден: {parser_path}")
        
        # Динамически загружаем модуль
        spec = importlib.util.spec_from_file_location("irecommend_parser", parser_path)
        parser_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(parser_module)
        
        logging.info("✅ Модуль парсера успешно загружен")
        return parser_module
        
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки модуля парсера: {e}")
        raise

def collect_data(**kwargs):
    """Сбор данных с помощью парсера и сохранение в CSV"""
    try:
        ti = kwargs['ti']
        
        # Загружаем модуль парсера
        parser_module = load_parser_module()
        
        # Создаем экземпляр парсера
        parser = parser_module.ReviewParser(BASE_URL)
        
        logging.info("🔄 Начинаем сбор данных с парсера...")
        
        # ЗАМЕНИТЕ ЭТУ СТРОКУ: используем правильный метод из парсера
        reviews = parser.scrape_reviews(start_url=START_URL, pages=TOTAL_PAGES)  # ИЗМЕНЕНИЕ ЗДЕСЬ
        
        if not reviews:
            logging.warning("⚠️ Парсер не вернул данные")
            return "no_data_collected"
        
        logging.info(f"✅ Собрано отзывов: {len(reviews)}")
        
        # Сохраняем в CSV
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = f"reviews_{timestamp}.csv"
        
        # Сохраняем в папку dags/files (создаем если нет)
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'files')
        os.makedirs(output_dir, exist_ok=True)
        
        csv_path = os.path.join(output_dir, csv_filename)
        
        # Используем метод сохранения из парсера
        parser.save_to_csv(reviews, csv_path)
        
        logging.info(f"💾 Данные сохранены в: {csv_path}")
        
        # Передаем путь к файлу через XCom
        ti.xcom_push(key='csv_path', value=csv_path)
        
        return f"collected_{len(reviews)}_reviews"
        
    except Exception as e:
        logging.error(f"❌ Ошибка при сборе данных: {e}")
        raise

def init_db():
    """Инициализация подключения к базе данных"""
    try:
        connection = psycopg2.connect(
            database='airflow', 
            user='airflow', 
            password='airflow',
            host='postgres',
            port=5432
        )
        logging.info("✅ Подключение к базе данных прошло успешно")
        
        query = "SELECT * FROM parser.reviews LIMIT 10"
        df = pd.read_sql(sql=query, con=connection)
        logging.info(f"Данные из БД:\n{df}")
            
        return "database_connection_success"
        
    except Exception as e:
        logging.error(f"❌ Ошибка подключения к базе данных: {e}")
        raise

def save_data_to_database(**kwargs):
    """Сохранение данных в базу данных"""
    try:
        ti = kwargs['ti']
        csv_path = ti.xcom_pull(task_ids='collect_data', key='csv_path')
        
        if not csv_path or not os.path.exists(csv_path):
            logging.error("❌ Файл с данными не найден")
            return "Файл с данными не найден"
            
        logging.info(f'📁 Файл с данными: {csv_path}')

        # Подключаемся к базе данных
        engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')
        
        # Читаем данные из CSV
        data = pd.read_csv(csv_path)
        logging.info(f"📊 Данные из CSV ({len(data)} строк):")
        logging.info(f"Колонки: {list(data.columns)}")
        
        # Подготовка данных
        data['scraped_at'] = pd.to_datetime(data['scraped_at'])

        data['combined_data'] = pd.to_datetime(data['date_created'] + ' ' + data['time_created'])

        max_date_from_table = pd.read_sql(sql = "select max(combined_created) from parser.reviews", con = engine)

        max_date = max_date_from_table['max'].iloc[0]

        data = data[
            (data['combined_data'] > max_date)
        ]

        # Сохраняем в базу
        data.to_sql(
            'reviews', 
            engine, 
            schema='parser', 
            if_exists='append', 
            index=False,
            method='multi'
        )
        
        logging.info(f"✅ Успешно сохранено {len(data)} записей в таблицу parser.raw_reviews")

        return f'Успешно сохранено {len(data)} записей в БД'
        
    except Exception as e:
        logging.error(f"❌ Ошибка при сохранении в БД: {e}")
        return f"Ошибка при сохранении в БД: {str(e)}"


with DAG(
    'parser_collect_data',
    description='Парсер данных с iRecommend',
    schedule_interval='0 2 * * *',  # Каждый день в 02:00,
    catchup=False,
    start_date = datetime(2025, 10, 18),
    tags=['parsing', 'data_collection'],
) as dag:
    
    start_task = EmptyOperator(task_id='start')
    
    init_db_task = PythonOperator(
        task_id='init_db', 
        python_callable=init_db,
        execution_timeout=timedelta(minutes=5)
    )
    
    collect_data_task = PythonOperator(
        task_id='collect_data',
        python_callable=collect_data,
        execution_timeout=timedelta(minutes=30),  # Парсинг может занять время
        provide_context=True
    )
    
    insert_data_task = PythonOperator(
        task_id='insert_data',
        python_callable=save_data_to_database,
        provide_context=True
    )

    
    end_task = EmptyOperator(task_id='end')

    # Определяем порядок выполнения
    start_task >> init_db_task >> collect_data_task >> insert_data_task >> end_task