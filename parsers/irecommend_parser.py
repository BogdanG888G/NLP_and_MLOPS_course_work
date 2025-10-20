import requests
from bs4 import BeautifulSoup
import time
import csv
import logging
import random
from urllib.parse import urljoin, urlparse
from datetime import datetime
import json
import cloudscraper
from fp.fp import FreeProxy

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
        self.session = None
        self.scraper = None
        self.setup_session()
        self.failed_requests = 0
        self.successful_requests = 0
        self.proxy_list = []
        self.current_proxy = None
        self.load_proxies()
        
    def load_proxies(self):
        """Загрузка списка прокси"""
        try:
            # Попробуем получить бесплатные прокси
            proxy = FreeProxy(anon=True, timeout=1).get()
            if proxy:
                self.proxy_list = [proxy]
                logging.info(f"Загружен прокси: {proxy}")
        except:
            logging.warning("Не удалось загрузить прокси")
            self.proxy_list = []

    def setup_session(self):
        """Настройка сессии с разными стратегиями"""
        # Пробуем cloudscraper сначала
        try:
            self.scraper = cloudscraper.create_scraper(
                browser={
                    'browser': 'chrome',
                    'platform': 'windows',
                    'mobile': False
                }
            )
            logging.info("Инициализирован cloudscraper")
        except Exception as e:
            logging.warning(f"Cloudscraper не доступен: {e}")
            self.scraper = None
        
        # Создаем обычную сессию как запасной вариант
        self.session = requests.Session()
        self.rotate_user_agent()
        
        self.session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://irecommend.ru/',
            'DNT': '1',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
        })

    def rotate_user_agent(self):
        """Смена User-Agent"""
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0',
        ]
        
        new_agent = random.choice(user_agents)
        if self.session:
            self.session.headers['User-Agent'] = new_agent
        logging.info(f"Сменен User-Agent: {new_agent[:50]}...")

    def rotate_proxy(self):
        """Смена прокси"""
        if not self.proxy_list:
            return False
            
        self.current_proxy = random.choice(self.proxy_list)
        logging.info(f"Используется прокси: {self.current_proxy}")
        return True

    def get_page(self, url, retries=3, delay=5):
        """Получение страницы с улучшенной обработкой Cloudflare"""
        for attempt in range(retries):
            try:
                # Случайная задержка
                sleep_time = random.uniform(delay, delay * 2)
                time.sleep(sleep_time)
                
                # Пробуем разные стратегии
                strategies = [
                    self._try_cloudscraper,
                    self._try_session_with_proxy,
                    self._try_session_direct
                ]
                
                for strategy in strategies:
                    html = strategy(url)
                    if html:
                        self.successful_requests += 1
                        return html
                    
                logging.warning(f"Все стратегии не сработали для {url}")
                    
            except Exception as e:
                logging.error(f"Ошибка при запросе {url} (попытка {attempt+1}): {e}")
            
            # Увеличиваем задержку между попытками
            if attempt < retries - 1:
                backoff_delay = delay * (attempt + 1) * random.uniform(1, 2)
                logging.info(f"Повтор через {backoff_delay:.2f} сек...")
                time.sleep(backoff_delay)
        
        logging.error(f"Не удалось загрузить страницу после {retries} попыток: {url}")
        self.failed_requests += 1
        return None

    def _try_cloudscraper(self, url):
        """Попытка через cloudscraper"""
        if not self.scraper:
            return None
            
        try:
            response = self.scraper.get(url, timeout=20)
            if response.status_code == 200:
                logging.info(f"Cloudscraper успешно загрузил: {url}")
                return response.text
        except Exception as e:
            logging.warning(f"Cloudscraper не сработал: {e}")
            
        return None

    def _try_session_with_proxy(self, url):
        """Попытка через сессию с прокси"""
        if not self.proxy_list:
            return None
            
        try:
            proxy = {'http': self.current_proxy, 'https': self.current_proxy}
            response = self.session.get(url, timeout=20, proxies=proxy)
            if response.status_code == 200:
                logging.info(f"Прокси успешно загрузил: {url}")
                return response.text
        except Exception as e:
            logging.warning(f"Прокси не сработал: {e}")
            self.rotate_proxy()
            
        return None

    def _try_session_direct(self, url):
        """Прямой запрос через сессию"""
        try:
            response = self.session.get(url, timeout=20)
            if response.status_code == 200:
                logging.info(f"Прямой запрос успешен: {url}")
                return response.text
            elif response.status_code == 521:
                logging.warning(f"Cloudflare блокировка (521) для {url}")
                self.rotate_user_agent()
        except Exception as e:
            logging.warning(f"Прямой запрос не сработал: {e}")
            
        return None

    def parse_reviews_from_list(self, html):
        """Парсинг отзывов со страницы"""
        soup = BeautifulSoup(html, 'html.parser')
        reviews_data = []
        
        # Ищем блоки с отзывами
        review_blocks = soup.find_all('div', class_='smTeaser')
        
        if not review_blocks:
            logging.warning("Не найдены блоки с отзывами")
            # Сохраним HTML для отладки
            with open(f'debug_{int(time.time())}.html', 'w', encoding='utf-8') as f:
                f.write(html)
            return []
        
        for block in review_blocks:
            review_data = self.parse_review_preview(block)
            if review_data:
                reviews_data.append(review_data)
        
        logging.info(f"Найдено отзывов: {len(reviews_data)}")
        return reviews_data

    def parse_review_preview(self, block):
        """Парсинг превью отзыва"""
        try:
            product_elem = block.find('div', class_='productName')
            product_name = product_elem.get_text(strip=True) if product_elem else "Неизвестный продукт"
            
            author_elem = block.find('div', class_='authorName')
            author_name = author_elem.get_text(strip=True) if author_elem else "Аноним"
            
            rating = self.extract_rating(block)
            
            date_elem = block.find('span', class_='date-created')
            time_elem = block.find('span', class_='time-created')
            date_created = date_elem.get_text(strip=True) if date_elem else ""
            time_created = time_elem.get_text(strip=True) if time_elem else ""
            
            title_elem = block.find('div', class_='reviewTitle')
            title = title_elem.get_text(strip=True) if title_elem else ""
            
            teaser_elem = block.find('span', class_='reviewTeaserText')
            teaser_text = teaser_elem.get_text(strip=True) if teaser_elem else ""
            
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
            logging.error(f"Ошибка парсинга превью: {e}")
            return None

    def extract_rating(self, block):
        """Извлечение рейтинга"""
        try:
            rating_elem = block.find('div', class_='starsRating')
            if rating_elem:
                for cls in rating_elem.get('class', []):
                    if 'fivestarWidgetStatic-' in cls:
                        return cls.split('-')[-1]
                
                stars = rating_elem.find_all('div', class_='star')
                filled = sum(1 for star in stars if star.find('div', class_='on'))
                return str(filled)
        except:
            pass
        return "0"

    def parse_full_review(self, url):
        """Парсинг полного отзыва"""
        if not url:
            return None
            
        time.sleep(random.uniform(2, 4))
        html = self.get_page(url)
        
        if not html:
            return None
            
        soup = BeautifulSoup(html, 'html.parser')
        review_block = soup.find('div', class_='reviewBlock')
        
        if not review_block:
            return None
            
        try:
            review_body = review_block.find('div', itemprop='reviewBody')
            full_text = self.clean_text(review_body) if review_body else ""
            
            experience = self.extract_experience(review_block)
            pluses = self.extract_pluses(review_block)
            minuses = self.extract_minuses(review_block)
            verdict = self.extract_verdict(review_block)
            
            return {
                'full_text': full_text,
                'experience': experience,
                'pluses': ' | '.join(pluses),
                'minuses': ' | '.join(minuses),
                'verdict': verdict,
            }
            
        except Exception as e:
            logging.error(f"Ошибка парсинга полного отзыва: {e}")
            return None

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
        """Извлечение вердикта"""
        try:
            verdict_elem = review_block.find('span', class_='verdict')
            return verdict_elem.get_text(strip=True) if verdict_elem else ""
        except:
            return ""

    def clean_text(self, element):
        """Очистка текста"""
        if not element:
            return ""
        
        for br in element.find_all("br"):
            br.replace_with("\n")
        
        text = element.get_text(separator='\n')
        lines = [line.strip() for line in text.split('\n')]
        lines = [line for line in lines if line]
        
        return '\n'.join(lines)

    def scrape_reviews(self, start_url, pages=2, max_reviews=20):
        """Основной метод сбора отзывов"""
        all_reviews = []
        
        for page in range(1, pages + 1):
            if page == 1:
                url = start_url
            else:
                url = f"{start_url}?page={page}"
            
            logging.info(f"Обработка страницы {page}: {url}")
            
            html = self.get_page(url)
            if not html:
                logging.warning(f"Не удалось загрузить страницу {page}")
                if page > 1 and len(all_reviews) > 0:
                    break
                continue
            
            previews = self.parse_reviews_from_list(html)
            
            if not previews:
                logging.warning(f"На странице {page} нет отзывов")
                break
            
            for i, preview in enumerate(previews, 1):
                if len(all_reviews) >= max_reviews:
                    break
                    
                logging.info(f"Обработка отзыва {i}/{len(previews)}")
                
                full_data = self.parse_full_review(preview['review_url'])
                
                if full_data:
                    complete_review = {**preview, **full_data}
                    all_reviews.append(complete_review)
                else:
                    preview['full_text'] = preview.get('teaser_text', '')
                    all_reviews.append(preview)
                
                time.sleep(random.uniform(1, 3))
            
            logging.info(f"Страница {page} обработана. Всего: {len(all_reviews)}")
            
            if len(all_reviews) >= max_reviews:
                break
                
            if page < pages:
                time.sleep(random.uniform(3, 6))
        
        logging.info(f"Парсинг завершен. Собрано: {len(all_reviews)}")
        return all_reviews

    def save_to_csv(self, reviews, filename):
        """Сохранение в CSV"""
        if not reviews:
            return False
            
        try:
            fieldnames = [
                'product_name', 'author', 'rating', 'date_created', 'time_created',
                'title', 'teaser_text', 'full_text', 'experience', 'pluses', 
                'minuses', 'verdict', 'review_url', 'scraped_at'
            ]
            
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for review in reviews:
                    row = {field: review.get(field, '') for field in fieldnames}
                    writer.writerow(row)
            
            logging.info(f"Сохранено в {filename}")
            return True
            
        except Exception as e:
            logging.error(f"Ошибка сохранения: {e}")
            return False

# Альтернативная стратегия - использование Selenium как запасного варианта
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    
    class SeleniumReviewParser(ReviewParser):
        """Парсер с Selenium как резервный вариант"""
        
        def __init__(self, base_url="https://irecommend.ru"):
            self.driver = None
            super().__init__(base_url)
            
        def setup_selenium(self):
            """Настройка Selenium"""
            try:
                options = Options()
                options.add_argument('--headless')
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                options.add_argument('--disable-blink-features=AutomationControlled')
                options.add_experimental_option("excludeSwitches", ["enable-automation"])
                options.add_experimental_option('useAutomationExtension', False)
                
                self.driver = webdriver.Chrome(options=options)
                self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                logging.info("Selenium драйвер инициализирован")
                return True
            except Exception as e:
                logging.error(f"Ошибка инициализации Selenium: {e}")
                return False
        
        def get_page_selenium(self, url):
            """Получение страницы через Selenium"""
            if not self.driver:
                if not self.setup_selenium():
                    return None
            
            try:
                self.driver.get(url)
                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                return self.driver.page_source
            except Exception as e:
                logging.error(f"Selenium ошибка: {e}")
                return None
        
        def __del__(self):
            if self.driver:
                self.driver.quit()
                
except ImportError:
    logging.warning("Selenium не установлен, используем только requests")

# Функция для Airflow с резервными стратегиями
def run_parser_safe():
    """Безопасный запуск парсера с несколькими стратегиями"""
    
    strategies = [
        # Пробуем основной парсер
        lambda: ReviewParser().scrape_reviews(
            "https://irecommend.ru/catalog/reviews/939-13393", 
            pages=1, 
            max_reviews=10
        ),
        
        # Пробуем с другим URL если основной не работает
        lambda: ReviewParser().scrape_reviews(
            "https://irecommend.ru/sitemap", 
            pages=1, 
            max_reviews=5
        ),
    ]
    
    # Если установлен Selenium, добавляем эту стратегию
    try:
        from selenium import webdriver
        strategies.append(
            lambda: SeleniumReviewParser().scrape_reviews(
                "https://irecommend.ru/catalog/reviews/939-13393",
                pages=1,
                max_reviews=10
            )
        )
    except:
        pass
    
    for i, strategy in enumerate(strategies, 1):
        try:
            logging.info(f"Попытка стратегии {i}")
            reviews = strategy()
            if reviews:
                logging.info(f"Стратегия {i} успешна, собрано {len(reviews)} отзывов")
                return reviews
        except Exception as e:
            logging.warning(f"Стратегия {i} не сработала: {e}")
            time.sleep(10)
    
    logging.error("Все стратегии не сработали")
    return []

def main():
    """Основная функция"""
    logging.info("Запуск парсера с резервными стратегиями...")
    
    reviews = run_parser_safe()
    
    if reviews:
        filename = f"reviews_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        parser = ReviewParser()
        parser.save_to_csv(reviews, filename)
        logging.info(f"Успешно собрано {len(reviews)} отзывов")
    else:
        logging.error("Не удалось собрать отзывы")

if __name__ == "__main__":
    main()