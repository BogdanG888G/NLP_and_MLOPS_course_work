import requests
from bs4 import BeautifulSoup
import time
import csv
import logging
import random
from urllib.parse import urljoin
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('review_parser.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class ReviewParser:
    def __init__(self, base_url="https://irecommend.ru"):
        self.base_url = base_url
        self.session = requests.Session()
        self.setup_session()
        
    def setup_session(self):
        """Настройка сессии с реалистичными заголовками"""
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://irecommend.ru/',
        })

    def get_page(self, url, retries=3, delay=2):
        """Получение страницы с обработкой ошибок"""
        for attempt in range(retries):
            try:
                time.sleep(random.uniform(delay, delay * 2))
                
                response = self.session.get(url, timeout=15)
                
                if response.status_code == 200:
                    # Проверяем, что получили контент с отзывами
                    if 'smTeaser' in response.text or 'reviewBlock' in response.text:
                        return response.text
                    else:
                        logging.warning(f"Страница {url} не содержит отзывов")
                        return None
                else:
                    logging.warning(f"Статус код {response.status_code} для {url}")
                    
            except Exception as e:
                logging.error(f"Ошибка при запросе {url} (попытка {attempt+1}): {e}")
            
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
        
        return None

    def parse_reviews_from_list(self, html):
        """Парсинг всех отзывов со страницы списка"""
        soup = BeautifulSoup(html, 'html.parser')
        reviews_data = []
        
        # Находим все блоки с отзывами
        review_blocks = soup.find_all('div', class_='smTeaser')
        
        for block in review_blocks:
            review_data = self.parse_review_preview(block)
            if review_data:
                reviews_data.append(review_data)
        
        logging.info(f"Найдено отзывов на странице: {len(reviews_data)}")
        return reviews_data

    def parse_review_preview(self, block):
        """Парсинг превью отзыва из списка"""
        try:
            # Основная информация
            product_elem = block.find('div', class_='productName')
            product_name = product_elem.get_text(strip=True) if product_elem else "Неизвестный продукт"
            
            author_elem = block.find('div', class_='authorName')
            author_name = author_elem.get_text(strip=True) if author_elem else "Аноним"
            
            # Рейтинг
            rating = self.extract_rating(block)
            
            # Дата и время
            date_elem = block.find('span', class_='date-created')
            time_elem = block.find('span', class_='time-created')
            date_created = date_elem.get_text(strip=True) if date_elem else ""
            time_created = time_elem.get_text(strip=True) if time_elem else ""
            
            # Заголовок и текст превью
            title_elem = block.find('div', class_='reviewTitle')
            title = title_elem.get_text(strip=True) if title_elem else ""
            
            teaser_elem = block.find('span', class_='reviewTeaserText')
            teaser_text = teaser_elem.get_text(strip=True) if teaser_elem else ""
            
            # Ссылка на полный отзыв
            link_elem = block.find('a', class_='reviewTextSnippet')
            review_url = urljoin(self.base_url, link_elem['href']) if link_elem and link_elem.get('href') else ""
            
            return {
                'product_name': product_name,
                'author': author_name,
                'rating': rating,
                'date_created': date_created,
                'time_created': time_created,
                'title': title,
                'teaser_text': teaser_text,
                'review_url': review_url,
                'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            logging.error(f"Ошибка при парсинге превью отзыва: {e}")
            return None

    def parse_full_review(self, url):
        """Парсинг полной версии отзыва"""
        html = self.get_page(url)
        if not html:
            return None
            
        soup = BeautifulSoup(html, 'html.parser')
        review_block = soup.find('div', class_='reviewBlock')
        
        if not review_block:
            logging.warning(f"Не найден блок отзыва для {url}")
            return None
            
        try:
            # Полный текст отзыва
            review_body = review_block.find('div', itemprop='reviewBody')
            full_text = self.clean_text(review_body) if review_body else ""
            
            # Дополнительная информация
            experience = self.extract_experience(review_block)
            pluses = self.extract_pluses(review_block)
            minuses = self.extract_minuses(review_block)
            verdict = self.extract_verdict(review_block)
            
            # Рейтинг из мета-тега
            rating_meta = review_block.find('meta', itemprop='ratingValue')
            rating = rating_meta['content'] if rating_meta else ""
            
            return {
                'full_text': full_text,
                'experience': experience,
                'pluses': ' | '.join(pluses),
                'minuses': ' | '.join(minuses),
                'verdict': verdict,
                'detailed_rating': rating
            }
            
        except Exception as e:
            logging.error(f"Ошибка при парсинге полного отзыва {url}: {e}")
            return None

    def extract_rating(self, block):
        """Извлечение рейтинга из звезд"""
        try:
            rating_elem = block.find('div', class_='starsRating')
            if rating_elem:
                # Ищем класс с рейтингом
                for cls in rating_elem.get('class', []):
                    if 'fivestarWidgetStatic-' in cls:
                        return cls.split('-')[-1]
                
                # Считаем заполненные звезды
                stars = rating_elem.find_all('div', class_='star')
                filled = sum(1 for star in stars if star.find('div', class_='on'))
                return str(filled)
        except Exception as e:
            logging.warning(f"Ошибка при извлечении рейтинга: {e}")
        
        return "0"

    def extract_experience(self, review_block):
        """Извлечение опыта использования"""
        try:
            experience_elem = review_block.find('div', class_='item-data')
            return experience_elem.get_text(strip=True) if experience_elem else ""
        except:
            return ""

    def extract_pluses(self, review_block):
        """Извлечение достоинств"""
        try:
            plus_block = review_block.find('div', class_='plus')
            if plus_block:
                plus_items = plus_block.find_all('li')
                return [item.get_text(strip=True) for item in plus_items]
        except:
            pass
        return []

    def extract_minuses(self, review_block):
        """Извлечение недостатков"""
        try:
            minus_block = review_block.find('div', class_='minus')
            if minus_block:
                minus_items = minus_block.find_all('li')
                return [item.get_text(strip=True) for item in minus_items]
        except:
            pass
        return []

    def extract_verdict(self, review_block):
        """Извлечение вердикта (рекомендует/не рекомендует)"""
        try:
            verdict_elem = review_block.find('span', class_='verdict')
            return verdict_elem.get_text(strip=True) if verdict_elem else ""
        except:
            return ""

    def clean_text(self, element):
        """Очистка текста от HTML тегов"""
        if not element:
            return ""
        
        # Сохраняем переносы строк
        for br in element.find_all("br"):
            br.replace_with("\n")
        
        text = element.get_text(separator='\n')
        
        # Очистка от лишних пробелов
        lines = [line.strip() for line in text.split('\n')]
        lines = [line for line in lines if line]
        
        return '\n'.join(lines)

    def scrape_reviews(self, start_url, pages=5):
        """Основной метод для сбора отзывов"""
        all_reviews = []
        
        for page in range(pages):
            if page == 0:
                url = start_url
            else:
                url = f"{start_url}?page={page}"
            
            logging.info(f"Обработка страницы {page + 1}: {url}")
            
            html = self.get_page(url)
            if not html:
                logging.warning(f"Не удалось загрузить страницу {page + 1}")
                continue
            
            # Парсим отзывы со страницы списка
            previews = self.parse_reviews_from_list(html)
            
            for i, preview in enumerate(previews, 1):
                logging.info(f"Обработка отзыва {i}/{len(previews)}: {preview['title'][:30]}...")
                
                # Получаем полный текст отзыва
                full_data = self.parse_full_review(preview['review_url'])
                
                if full_data:
                    # Объединяем данные
                    complete_review = {**preview, **full_data}
                    all_reviews.append(complete_review)
                else:
                    # Сохраняем хотя бы превью
                    preview['full_text'] = preview.get('teaser_text', '')
                    all_reviews.append(preview)
                
                # Задержка между запросами
                time.sleep(random.uniform(1, 3))
            
            logging.info(f"Страница {page + 1} обработана. Всего отзывов: {len(all_reviews)}")
            
            # Задержка между страницами
            if page < pages - 1:
                time.sleep(random.uniform(2, 4))
        
        return all_reviews

    def save_to_csv(self, reviews, filename):
        """Сохранение отзывов в CSV файл"""
        if not reviews:
            logging.error("Нет данных для сохранения")
            return False
            
        try:
            # Определяем все возможные поля
            fieldnames = [
                'product_name', 'author', 'rating', 'date_created', 'time_created',
                'title', 'teaser_text', 'full_text', 'experience', 'pluses', 
                'minuses', 'verdict', 'detailed_rating', 'review_url', 'scraped_at'
            ]
            
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for review in reviews:
                    # Записываем только существующие поля
                    row = {field: review.get(field, '') for field in fieldnames}
                    writer.writerow(row)
            
            logging.info(f"Успешно сохранено {len(reviews)} отзывов в {filename}")
            return True
            
        except Exception as e:
            logging.error(f"Ошибка при сохранении в CSV: {e}")
            return False

# Пример использования
def main():
    # URL страницы с отзывами (замените на актуальный)
    START_URL = "https://irecommend.ru/catalog/reviews/939-13393"
    
    parser = ReviewParser()
    
    logging.info("Начало парсинга отзывов...")
    
    # Собираем отзывы (например, с 3 страниц)
    reviews = parser.scrape_reviews(START_URL, pages=1)
    
    if reviews:
        # Сохраняем в CSV
        filename = f"reviews_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        parser.save_to_csv(reviews, filename)
        
        # Выводим статистику
        logging.info(f"Собрано отзывов: {len(reviews)}")
        logging.info(f"Первый отзыв: {reviews[0]['title']}")
        logging.info(f"Текст первого отзыва: {reviews[0]['full_text'][:100]}...")
    else:
        logging.error("Не удалось собрать отзывы")

if __name__ == "__main__":
    main()