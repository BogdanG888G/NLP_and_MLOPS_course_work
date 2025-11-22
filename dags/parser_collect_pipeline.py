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
from sqlalchemy import create_engine

# Настройки
BASE_URL = "https://irecommend.ru"
START_URL = "https://irecommend.ru/catalog/reviews/939-13393"
TOTAL_PAGES = 1  # Количество страниц для парсинга

# Настройка логирования
logging.basicConfig(level=logging.INFO)

def load_parser_module():
    """Динамическая загрузка модуля парсера"""
    try:
        # Путь к парсеру в корне проекта (на уровень выше dags)
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        parser_path = os.path.join(project_root, "parsers", "irecommend_parser.py")
        
        logging.info(f"🔍 Ищем парсер по пути: {parser_path}")
        
        if not os.path.exists(parser_path):
            # Альтернативный путь - прямо в корне
            parser_path = os.path.join(project_root, "irecommend_parser.py")
            logging.info(f"🔍 Пробуем альтернативный путь: {parser_path}")
            
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
        
        logging.info(f"🔄 Начинаем сбор данных с {START_URL}, страниц: {TOTAL_PAGES}")
        
        # Используем метод scrape_reviews для сбора отзывов
        reviews = parser.scrape_reviews(start_url=START_URL, pages=TOTAL_PAGES)
        
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
        success = parser.save_to_csv(reviews, csv_path)
        
        if success:
            logging.info(f"💾 Данные сохранены в: {csv_path}")
            
            # Передаем путь к файлу через XCom
            ti.xcom_push(key='csv_path', value=csv_path)
            
            return f"collected_{len(reviews)}_reviews"
        else:
            logging.error("❌ Ошибка при сохранении в CSV")
            return "csv_save_failed"
        
    except Exception as e:
        logging.error(f"❌ Ошибка при сборе данных: {e}")
        raise


def save_data_to_database(**kwargs):
    """Сохранение данных в базу данных"""
    try:
        ti = kwargs['ti']
        csv_path = ti.xcom_pull(task_ids='collect_data', key='csv_path')
        
        if not csv_path or not os.path.exists(csv_path):
            logging.error("❌ Файл с данными не найден")
            return "data_file_not_found"
            
        logging.info(f'📁 Загружаем данные из файла: {csv_path}')

        # Подключаемся к базе данных
        engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')
        
        # Читаем данные из CSV
        data = pd.read_csv(csv_path, encoding='utf-8')
        logging.info(f"📊 Загружено {len(data)} строк из CSV")
        logging.info(f"Колонки: {list(data.columns)}")
        
        # Подготовка данных
        data['scraped_at'] = pd.to_datetime(data['scraped_at'], errors='coerce')
        
        # Создаем combined_created из date_created и time_created
        if 'date_created' in data.columns and 'time_created' in data.columns:
            data['combined_created'] = pd.to_datetime(
                data['date_created'] + ' ' + data['time_created'], 
                errors='coerce',
                format='%d.%m.%Y %H:%M'
            )
        else:
            data['combined_created'] = pd.NaT
        '''
        # Получаем максимальную дату из таблицы
        try:
            max_date_query = "SELECT MAX(combined_created) as max_date FROM parser.reviews"
            max_date_from_table = pd.read_sql(sql=max_date_query, con=engine)
            max_date = max_date_from_table['max_date'].iloc[0]
            
            logging.info(f"📅 Максимальная дата в таблице: {max_date}")
            
            # Фильтруем только новые данные
            if max_date is not pd.NaT and max_date is not None:
                data = data[data['combined_created'] > max_date]
                logging.info(f"🆕 Новых записей для добавления: {len(data)}")
            else:
                logging.info("🎯 Таблица пустая или максимальная дата не определена, добавляем все записи")
        except Exception as e:
            logging.warning(f"⚠️ Ошибка при проверке максимальной даты: {e}")
            logging.info("🔄 Добавляем все записи")
        
        if len(data) == 0:
            logging.info("ℹ️ Нет новых данных для добавления")
            return "no_new_data"'''
        
        # Сохраняем в базу
        data.to_sql(
            'reviews', 
            engine, 
            schema='parser', 
            if_exists='append', 
            index=False
        )
        
        logging.info(f"✅ Успешно сохранено {len(data)} записей в таблицу parser.reviews")
        
        # Очищаем временный файл
        try:
            os.remove(csv_path)
            logging.info(f"🧹 Временный файл {csv_path} удален")
        except:
            logging.warning(f"⚠️ Не удалось удалить временный файл {csv_path}")
        
        return f'saved_{len(data)}_records_to_db'
        
    except Exception as e:
        logging.error(f"❌ Ошибка при сохранении в БД: {e}")
        return f"db_save_error: {str(e)}"

with DAG(
    'irecommend_parser_dag',
    start_date=datetime(2025, 1, 18),
    description='Парсер отзывов с iRecommend',
    schedule_interval='0 2 * * *',  
    catchup=False,
    tags=['parsing', 'irecommend', 'reviews'],
) as dag:
    
    start_task = EmptyOperator(task_id='start')
    
    collect_data_task = PythonOperator(
        task_id='collect_data',
        python_callable=collect_data,
        execution_timeout=timedelta(minutes=30),
        provide_context=True
    )
    
    insert_data_task = PythonOperator(
        task_id='insert_data',
        python_callable=save_data_to_database,
        provide_context=True
    )
    
    end_task = EmptyOperator(task_id='end')

    # Определяем порядок выполнения
    start_task >> collect_data_task >> insert_data_task >> end_task