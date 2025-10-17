import requests
from bs4 import BeautifulSoup
import time
import json
from urllib.parse import urljoin
import re
import pandas as pd
import logging
import random

class ReviewParser:
    def __init__(self, base_url):
        self.base_url = base_url
        self.session = requests.Session()
        
        # Более реалистичные заголовки
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
        })

    def get_page(self, url, retries=3, delay=5):
        for attempt in range(retries):
            try:
                # Добавляем случайные задержки между запросами
                time.sleep(random.uniform(2, 5))
                
                response = self.session.get(
                    url, 
                    timeout=30,
                    allow_redirects=True
                )
                
                if response.status_code == 521:
                    logging.info(f"Сервер вернул ошибку 521 (CloudFlare) для {url}. Попытка {attempt + 1}/{retries}")
                    
                    # Пробуем обойти CloudFlare
                    if self.bypass_cloudflare(url):
                        continue
                        
                    if attempt < retries - 1:
                        time.sleep(delay * (attempt + 1))  # Увеличиваем задержку
                        continue
                    else:
                        response.raise_for_status()
                
                # Проверяем, что получили HTML, а не страницу с капчей
                if 'cf-challenge' in response.text or 'cloudflare' in response.text.lower():
                    logging.warning(f"Обнаружена CloudFlare защита для {url}")
                    if attempt < retries - 1:
                        time.sleep(delay * 2)
                        continue
                
                response.raise_for_status()
                return response.text
                
            except requests.RequestException as e:
                logging.error(f"Ошибка при получении страницы {url} (попытка {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(delay * (attempt + 1))
                else:
                    return None
        return None

    def bypass_cloudflare(self, url):
        """Пытаемся обойти CloudFlare защиту"""
        try:
            # Меняем User-Agent
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            ]
            self.session.headers['User-Agent'] = random.choice(user_agents)
            
            # Добавляем реферер
            self.session.headers['Referer'] = 'https://irecommend.ru/'
            
            return True
        except Exception as e:
            logging.error(f"Ошибка при обходе CloudFlare: {e}")
            return False
    
    def get_page(self, url, retries=3, delay=2):
        for attempt in range(retries):
            try:
                response = self.session.get(url)
                
                if response.status_code == 521:
                    logging.info(f"Сервер вернул ошибку 521 для {url}. Попытка {attempt + 1}/{retries}")
                    if attempt < retries - 1:
                        time.sleep(delay)
                        continue
                    else:
                        response.raise_for_status()
                
                response.raise_for_status()
                return response.text
                
            except requests.RequestException as e:
                logging.error(f"Ошибка при получении страницы {url} (попытка {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(delay)
                else:
                    return None
        return None
    
    def parse_reviews_list(self, html):
        """Парсинг списка отзывов на главной странице"""
        soup = BeautifulSoup(html, 'html.parser')
        reviews = []
        
        # Находим все блоки с отзывами
        review_blocks = soup.find_all('div', class_='smTeaser')
        
        for block in review_blocks:
            review_data = self.parse_review_preview(block)
            if review_data:
                reviews.append(review_data)
        
        return reviews
    
    def extract_datetime_from_text(self, date_text):
        """Извлечение даты и времени из текста (пример: '14 Октябрь, 2025 - 15:53')"""
        if not date_text:
            return ""
        
        try:
            # Пример: "14 Октябрь, 2025 - 15:53" -> "14.10.25 15:53"
            # Словарь для преобразования месяцев
            months = {
                'январь': '01', 'февраль': '02', 'март': '03', 'апрель': '04',
                'май': '05', 'июнь': '06', 'июль': '07', 'август': '08',
                'сентябрь': '09', 'октябрь': '10', 'ноябрь': '11', 'декабрь': '12'
            }
            
            # Разбираем строку
            parts = date_text.split(' - ')
            if len(parts) == 2:
                date_part = parts[0].strip()  # "14 Октябрь, 2025"
                time_part = parts[1].strip()  # "15:53"
                
                # Разбираем дату
                date_parts = date_part.split()
                if len(date_parts) >= 3:
                    day = date_parts[0].zfill(2)  # "14"
                    month_ru = date_parts[1].rstrip(',').lower()  # "октябрь"
                    year = date_parts[2]  # "2025"
                    
                    # Преобразуем месяц
                    month = months.get(month_ru, '01')
                    
                    # Сокращаем год до двух цифр
                    year_short = year[2:] if len(year) == 4 else year
                    
                    return f"{day}.{month}.{year_short} {time_part}"
            
            return date_text  # Если не удалось распарсить, возвращаем исходный текст
            
        except Exception as e:
            logging.warning(f"Ошибка при преобразовании даты '{date_text}': {e}")
            return date_text

    def parse_review_preview(self, block):
        """Парсинг превью отзыва"""
        try:
            # Основная информация из превью
            product_name = block.find('div', class_='productName').get_text(strip=True)
            author_name = block.find('div', class_='authorName').get_text(strip=True)
            
            # Рейтинг
            rating_elem = block.find('div', class_='starsRating')
            rating = self.extract_rating(rating_elem)
            
            # Дата и время
            date_created = block.find('span', class_='date-created').get_text(strip=True)
            time_created = block.find('span', class_='time-created').get_text(strip=True)
            
            # Создаем общее поле даты и времени публикации
            published_datetime = f"{date_created} {time_created}"
            
            # Заголовок и текст
            title = block.find('div', class_='reviewTitle').get_text(strip=True)
            teaser = block.find('span', class_='reviewTeaserText').get_text(strip=True)
            
            # Ссылка на полный отзыв
            review_link = block.find('a', class_='reviewTextSnippet')['href']
            full_review_url = urljoin(self.base_url, review_link)
            
            # Количество комментариев
            comments_elem = block.find('div', class_='comments')
            comments_count = comments_elem.get_text(strip=True) if comments_elem else "0"
            
            # Фотографии
            photo_count = block.get('data-photos-count', '0')
            
            return {
                'product_name': product_name,
                'author': author_name,
                'rating': rating,
                'date': date_created,
                'time': time_created,
                'published_datetime': published_datetime,
                'title': title,
                'teaser_text': teaser,
                'full_review_url': full_review_url,
                'comments_count': comments_count,
                'photos_count': photo_count,
                'scraped_at': time.strftime('%Y-%m-%d %H:%M:%S')
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
            return None
            
        try:
            # Основная информация
            author_elem = review_block.find('span', itemprop='name')
            author = author_elem.get_text(strip=True) if author_elem else ""
            
            # Рейтинг
            rating_meta = review_block.find('meta', itemprop='ratingValue')
            rating = rating_meta['content'] if rating_meta else ""
            
            # Дата публикации
            date_meta = review_block.find('meta', itemprop='datePublished')
            published_date = date_meta['content'] if date_meta else ""
            
            # Дата и время из текста (для published_datetime)
            date_text_elem = review_block.find('span', class_='dtreviewed')
            date_text = date_text_elem.get_text(strip=True) if date_text_elem else ""
            
            # Извлекаем дату и время из текста (пример: "14 Октябрь, 2025 - 15:53")
            published_datetime = self.extract_datetime_from_text(date_text)
            
            # Заголовок
            title_elem = review_block.find('h2', class_='reviewTitle')
            title = title_elem.get_text(strip=True) if title_elem else ""
            
            # Стоимость
            price_elem = review_block.find('div', class_='item-data')
            price = price_elem.get_text(strip=True) if price_elem else ""
            
            # Полный текст отзыва
            review_body = review_block.find('div', itemprop='reviewBody')
            full_text = self.clean_review_text(review_body) if review_body else ""
            
            # Изображения (сохраняем как строку с разделителем для CSV)
            images = []
            img_elements = review_block.find_all('img')
            for img in img_elements:
                src = img.get('src') or img.get('data-original')
                if src and 'user-images' in src:
                    images.append(urljoin(self.base_url, src))
            
            # Вердикт (рекомендует/не рекомендует)
            verdict_elem = review_block.find('span', class_='verdict')
            verdict = verdict_elem.get_text(strip=True) if verdict_elem else ""
            
            return {
                'author': author,
                'rating': rating,
                'published_date': published_date,
                'published_datetime': published_datetime,
                'title': title,
                'price': price,
                'full_text': full_text,
                'images': ' | '.join(images),  # Сохраняем как строку для CSV
                'verdict': verdict,
                'review_url': url
            }
            
        except Exception as e:
            logging.error(f"Ошибка при парсинге полного отзыва {url}: {e}")
            return None
    
    def extract_rating(self, rating_elem):
        """Извлечение рейтинга из звезд"""
        if not rating_elem:
            return 0
        
        # Ищем классы с рейтингом
        rating_classes = rating_elem.get('class', [])
        for cls in rating_classes:
            if 'fivestarWidgetStatic-' in cls:
                try:
                    return int(cls.split('-')[-1])
                except ValueError:
                    continue
        
        # Альтернативный способ: считаем заполненные звезды
        stars = rating_elem.find_all('div', class_='star')
        filled_stars = len([star for star in stars if star.find('div', class_='on')])
        return filled_stars
    
    def clean_review_text(self, review_body):
        """Очистка текста от HTML тегов и лишних пробелов"""

        if not review_body:
            return ""
        
        # Удаляем все теги, кроме переносов строк
        for br in review_body.find_all("br"):
            br.replace_with("\n")
        
        text = review_body.get_text(separator='\n')
        
        # Очищаем от лишних пробелов и переносов
        lines = [line.strip() for line in text.split('\n')]
        lines = [line for line in lines if line]
        
        return '\n'.join(lines)
    
    def get_reviews_from_page(self, page_url):
        """Получить отзывы с одной страницы"""
        logging.info(f"Парсинг страницы: {page_url}")
        
        html = self.get_page(page_url)
        if not html:
            logging.error(f"Не удалось загрузить страницу: {page_url}")
            return []
            
        # Парсим отзывы с текущей страницы
        page_reviews = self.parse_reviews_list(html)
        logging.info(f"Найдено отзывов на странице: {len(page_reviews)}")
        
        all_reviews = []
        
        # Для каждого отзыва получаем полную версию
        for i, review_preview in enumerate(page_reviews, 1):
            logging.info(f"Парсинг полного отзыва {i}/{len(page_reviews)}: {review_preview['title'][:50]}...")
            
            full_review = self.parse_full_review(review_preview['full_review_url'])
            if full_review:
                # Объединяем данные из превью и полного отзыва
                complete_review = {**review_preview, **full_review}
                all_reviews.append(complete_review)
            else:
                # Если не удалось получить полный отзыв, сохраняем хотя бы превью
                logging.error(f"Не удалось получить полный отзыв, сохраняем превью: {review_preview['title'][:50]}...")
                # Добавляем обязательные поля из полного отзыва с пустыми значениями
                review_preview['published_date'] = ""
                review_preview['review_url'] = review_preview['full_review_url']
                review_preview['price'] = ""
                review_preview['full_text'] = ""
                review_preview['images'] = ""
                review_preview['verdict'] = ""
                all_reviews.append(review_preview)
            
            # Задержка между запросами
            time.sleep(5)
        
        return all_reviews
    
    def run_parsing(self, total_pages=20):
        """Основной метод для запуска парсинга всех страниц"""
        logging.info(f"Запуск парсинга {total_pages} страниц")
        all_reviews = []
        successful_pages = 0
        
        for page_num in range(total_pages):
            # Формируем URL страницы
            if page_num == 0:
                page_url = "https://irecommend.ru/catalog/reviews/939-13393"
            else:
                page_url = f"https://irecommend.ru/catalog/reviews/939-13393?page={page_num}"
            
            logging.info(f"=== Страница {page_num + 1}/{total_pages} ===")
            
            # Получаем отзывы со страницы
            page_reviews = self.get_reviews_from_page(page_url)
            
            if page_reviews:
                all_reviews.extend(page_reviews)
                successful_pages += 1
                logging.info(f"Успешно собрано отзывов со страницы: {len(page_reviews)}")
            else:
                logging.error(f"Не удалось собрать отзывы со страницы {page_num + 1}")
            
            logging.info(f"Всего собрано отзывов: {len(all_reviews)}")
            
            # Задержка между страницами
            time.sleep(3)
        
        logging.info(f"=== Сбор завершен ===")
        logging.info(f"Обработано страниц: {successful_pages}/{total_pages}")
        logging.info(f"Всего собрано отзывов: {len(all_reviews)}")
        
        return all_reviews
    
    def save_to_csv(self, reviews, filename):
        """Сохранить отзывы в CSV файл"""
        import csv
        
        if not reviews:
            logging.error("Нет данных для сохранения в CSV")
            return
        
        # Предопределенный список всех возможных полей
        fieldnames = [
            'product_name', 'author', 'rating', 'date', 'time', 'published_datetime',
            'title', 'teaser_text', 'full_review_url', 'comments_count', 'photos_count',
            'scraped_at', 'published_date', 'review_url', 'price', 'full_text', 
            'images', 'verdict'
        ]
        
        with open(filename, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(reviews)
        logging.info(f"Отзывы сохранены в {filename}")


