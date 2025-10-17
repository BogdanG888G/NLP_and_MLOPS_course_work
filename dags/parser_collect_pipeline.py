from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import logging
import os
import sys

# Добавляем путь к папке parsers в PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'parsers'))
sys.path.insert(0, '/opt/airflow/parsers')  # альтернативный путь

try:
    from irecommend_parser import ReviewParser
    logging.info("✅ ReviewParser успешно импортирован")
except ImportError as e:
    logging.error(f"❌ Ошибка импорта ReviewParser: {e}")
    logging.info(f"Python path: {sys.path}")
    logging.info(f"Текущая директория: {os.getcwd()}")
    logging.info(f"Содержимое parsers директории: {os.listdir('/opt/airflow/parsers')}")

BASE_URL = "https://irecommend.ru"
TOTAL_PAGES = 2  # Для теста уменьшим количество страниц

def init_db():
    """Инициализация подключения к базе данных"""
    try:
        engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')
        logging.info("✅ Подключение к базе данных прошло успешно")
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 'HELLO, AIRFLOW!' as greeting"))
            row = result.fetchone()
            if row:
                greeting = row[0]
                logging.info(f"Результат запроса: {greeting}")
            else:
                logging.info("Запрос не вернул результатов")
            
        return "database_connection_success"
        
    except Exception as e:
        logging.error(f"❌ Ошибка подключения к базе данных: {e}")
        raise

def parse_reviews():
    """Функция для парсинга отзывов"""
    try:
        logging.info("🔄 Запуск парсера...")
        
        parser = ReviewParser(BASE_URL)
        logging.info(f"🔄 Парсим {TOTAL_PAGES} страниц...")
        
        # Используем тестовый режим
        reviews = parser.run_parsing(total_pages=TOTAL_PAGES)
        
        if reviews:
            os.makedirs('/opt/airflow/data/reviews', exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_path = f'/opt/airflow/data/reviews/reviews_{timestamp}.csv'
            parser.save_to_csv(reviews, csv_path)
            
            logging.info(f"✅ Парсинг завершен. Собрано {len(reviews)} отзывов")
            logging.info(f"💾 Файл сохранен: {csv_path}")
            
            return f"Успешно собрано {len(reviews)} отзывов"
        else:
            logging.warning("⚠️ Парсинг не дал результатов")
            return "Собрано 0 отзывов"
            
    except Exception as e:
        logging.error(f"❌ Ошибка при парсинге: {e}")
        raise

# Определение DAG
with DAG(
    'parser_collect_data', 
    description='Парсер данных с iRecommend',
    start_date=datetime(2025, 10, 17),
    schedule_interval=None,
    catchup=False,
    tags=['parsing', 'data_collection'],
) as dag:
    
    start_task = EmptyOperator(
        task_id='start'
    )

    init_db_task = PythonOperator(
        task_id='init_db', 
        python_callable=init_db,
        execution_timeout=timedelta(minutes=5)
    )

    collect_data_task = PythonOperator(
        task_id="collect_data",
        python_callable=parse_reviews,
        execution_timeout=timedelta(minutes=5)
    )

    end_task = EmptyOperator(
        task_id='end'
    )

    start_task >> init_db_task >> collect_data_task >> end_task