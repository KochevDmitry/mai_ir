
"""
Парсер статей F1News.ru
Собирает HTML страницы статей и сохраняет в PostgreSQL
Поддерживает остановку/возобновление и переобкачку изменённых документов
"""

import requests
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
import psycopg2
import yaml
import sys
import hashlib


def load_config(config_path):
    """Загружает конфигурацию из YAML файла"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        print(f"✓ Конфиг загружен: {config_path}")
        return config
    except Exception as e:
        print(f"✗ Ошибка загрузки конфига: {e}")
        sys.exit(1)


def get_db_connection(db_config):
    """Создает соединение с базой данных"""
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        return conn
    except Exception as e:
        print(f"✗ Ошибка подключения к БД: {e}")
        return None


def init_database(db_config):
    """Инициализирует таблицы базы данных"""
    conn = get_db_connection(db_config)
    if not conn:
        return False

    try:
        cursor = conn.cursor()

        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS articles (
                id SERIAL PRIMARY KEY,
                url TEXT NOT NULL UNIQUE,
                html_content TEXT NOT NULL,
                source_name TEXT,
                crawl_timestamp BIGINT,
                crawl_date TEXT,
                filename TEXT,
                article_number INTEGER
            )
        ''')

        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS parser_state (
                id SERIAL PRIMARY KEY,
                source_name TEXT NOT NULL UNIQUE,
                current_date TEXT,
                articles_count INTEGER,
                last_update BIGINT
            )
        ''')

        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS article_hashes (
                url TEXT PRIMARY KEY,
                content_hash TEXT NOT NULL,
                last_checked BIGINT NOT NULL
            )
        ''')

        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_url ON articles(url)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source_name)')

        conn.commit()
        cursor.close()
        conn.close()
        print("✓ База данных инициализирована")
        return True
    except Exception as e:
        print(f"✗ Ошибка инициализации БД: {e}")
        return False


def get_content_hash(html_content):
    """Вычисляет MD5 хеш контента для определения изменений"""
    return hashlib.md5(html_content.encode('utf-8')).hexdigest()


def save_parser_state(db_config, source_name, current_date, articles_count):
    """Сохраняет текущее состояние парсера (checkpoint)"""
    conn = get_db_connection(db_config)
    if not conn:
        return False

    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO parser_state (source_name, current_date, articles_count, last_update)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (source_name) 
            DO UPDATE SET 
                current_date = EXCLUDED.current_date,
                articles_count = EXCLUDED.articles_count,
                last_update = EXCLUDED.last_update
        ''', (source_name, current_date, articles_count, int(time.time())))

        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"  ✗ Ошибка сохранения состояния: {e}")
        return False


def load_parser_state(db_config, source_name):
    """Загружает сохраненное состояние парсера"""
    conn = get_db_connection(db_config)
    if not conn:
        return None

    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT current_date, articles_count 
            FROM parser_state 
            WHERE source_name = %s
        ''', (source_name,))

        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if result:
            return {'current_date': result[0], 'articles_count': result[1]}
        return None
    except Exception as e:
        print(f"  ✗ Ошибка загрузки состояния: {e}")
        return None


def get_articles_count(db_config, source_name):
    """Возвращает количество статей в базе"""
    conn = get_db_connection(db_config)
    if not conn:
        return 0

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM articles WHERE source_name = %s", (source_name,))
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count
    except Exception as e:
        print(f"✗ Ошибка при подсчете статей: {e}")
        return 0


def get_article_hash(db_config, url):
    """Получает сохраненный хеш статьи"""
    conn = get_db_connection(db_config)
    if not conn:
        return None

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT content_hash FROM article_hashes WHERE url = %s", (url,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if result:
            return result[0]
        return None
    except Exception as e:
        print(f"  ✗ Ошибка получения хеша: {e}")
        return None


def save_article_hash(db_config, url, content_hash):
    """Сохраняет хеш статьи"""
    conn = get_db_connection(db_config)
    if not conn:
        return False

    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO article_hashes (url, content_hash, last_checked)
            VALUES (%s, %s, %s)
            ON CONFLICT (url) 
            DO UPDATE SET 
                content_hash = EXCLUDED.content_hash,
                last_checked = EXCLUDED.last_checked
        ''', (url, content_hash, int(time.time())))

        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"  ✗ Ошибка сохранения хеша: {e}")
        return False


def article_exists(db_config, url):
    """Проверяет, существует ли статья в базе"""
    conn = get_db_connection(db_config)
    if not conn:
        return False

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM articles WHERE url = %s", (url,))
        exists = cursor.fetchone() is not None
        cursor.close()
        conn.close()
        return exists
    except Exception as e:
        print(f"  ✗ Ошибка при проверке существования: {e}")
        return False


def save_article_to_db(db_config, url, html_content, article_number, source_name='f1news.ru'):
    """Сохраняет статью в базу данных"""
    conn = get_db_connection(db_config)
    if not conn:
        return False

    try:
        cursor = conn.cursor()

        
        date_published = "unknown"
        match = re.search(r'news/(\d{4}/\d{2}/\d{2})/', url)
        if match:
            date_published = match.group(1)

        
        url_path = url.replace('https://www.f1news.ru', '').strip('/')
        filename = url_path.replace('/', '_')
        if not filename.endswith('.html'):
            filename += '.html'
        filename = re.sub(r'[<>:"\\|?*]', '_', filename)

        current_time = int(time.time())

        
        cursor.execute('''
            INSERT INTO articles (url, html_content, source_name, crawl_timestamp, crawl_date, filename, article_number)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) 
            DO UPDATE SET 
                html_content = EXCLUDED.html_content,
                crawl_timestamp = EXCLUDED.crawl_timestamp
        ''', (
            url,
            html_content,
            source_name,
            current_time,
            date_published,
            filename,
            article_number
        ))

        conn.commit()
        cursor.close()
        conn.close()

        
        content_hash = get_content_hash(html_content)
        save_article_hash(db_config, url, content_hash)

        return True
    except Exception as e:
        print(f"  ✗ Ошибка сохранения в БД: {e}")
        return False


def extract_article_links(base_url, date_str, delay):
    """Извлекает ссылки на статьи за определенную дату"""
    url = f"{base_url}/news/{date_str}/"

    try:
        time.sleep(delay)

        response = requests.get(url, timeout=15)

        if response.status_code == 404:
            return []

        if response.status_code != 200:
            print(f"  ✗ HTTP {response.status_code} для {url}")
            return []

        soup = BeautifulSoup(response.content, 'html.parser')

        
        articles = set()

        all_links = soup.find_all('a', href=True)
        for link in all_links:
            href = link['href']
            if '/news/' in href and date_str.replace('/', '/') in href:
                full_url = urljoin(base_url, href)
                articles.add(full_url)

        
        news_items = soup.find_all(['div', 'article', 'li'], class_=re.compile(r'news|article|item', re.I))
        for item in news_items:
            links = item.find_all('a', href=True)
            for link in links:
                href = link['href']
                if '/news/' in href:
                    full_url = urljoin(base_url, href)
                    articles.add(full_url)

        return list(articles)

    except Exception as e:
        print(f"  ✗ Ошибка при парсинге {url}: {e}")
        return []


def download_and_save_article(db_config, url, article_number, delay, source_name='f1news.ru', check_updates=False):
    """Скачивает и сохраняет статью в БД (или обновляет если изменилась)"""
    try:
        time.sleep(delay)

        response = requests.get(url, timeout=20)

        if response.status_code != 200:
            print(f"  ✗ HTTP {response.status_code} для {url}")
            return False

        html_content = response.text
        new_hash = get_content_hash(html_content)

        
        exists = article_exists(db_config, url)

        if exists:
            if check_updates:
                
                old_hash = get_article_hash(db_config, url)
                if old_hash and old_hash != new_hash:
                    if save_article_to_db(db_config, url, html_content, article_number, source_name):
                        return True
                else:
                    print(f"  → Без изменений: {url}")
                    return False
            else:
                print(f"  → Уже в БД: {url}")
                return True
        else:
            
            if save_article_to_db(db_config, url, html_content, article_number, source_name):
                return True

        return False

    except requests.exceptions.RequestException as e:
        print(f"  ✗ Ошибка загрузки {url}: {e}")
        return False
    except Exception as e:
        print(f"  ✗ Неизвестная ошибка: {e}")
        return False


def get_urls_for_recrawl(db_config, source_name, recrawl_interval, limit=10):
    """Получает URL для переобкачки (старые по времени проверки)"""
    if recrawl_interval <= 0:
        return []

    conn = get_db_connection(db_config)
    if not conn:
        return []

    try:
        cursor = conn.cursor()

        
        threshold = int(time.time()) - recrawl_interval
        cursor.execute('''
            SELECT a.url 
            FROM articles a
            LEFT JOIN article_hashes h ON a.url = h.url
            WHERE a.source_name = %s 
            AND (h.last_checked IS NULL OR h.last_checked < %s)
            LIMIT %s
        ''', (source_name, threshold, limit))

        urls = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()

        return urls
    except Exception as e:
        print(f"  ✗ Ошибка получения URL для переобкачки: {e}")
        return []


def date_to_str(date_obj):
    """Преобразует дату в строку YYYY/MM/DD"""
    return date_obj.strftime("%Y/%m/%d")


def main():
    """Основная функция"""

    
    if len(sys.argv) < 2:
        print("Использование: python parser.py <путь_к_конфигу.yaml>")
        sys.exit(1)

    config_path = sys.argv[1]

    
    config = load_config(config_path)

    db_config = config['db']
    logic_config = config['logic']

    base_url = logic_config.get('base_url', 'https://www.f1news.ru')
    source_name = logic_config.get('source_name', 'f1news.ru')
    delay = logic_config.get('delay', 1.0)
    target_count = logic_config.get('target_count', 30000)
    max_empty_days = logic_config.get('max_empty_days', 30)
    recrawl_interval = logic_config.get('recrawl_interval', 0)  
    recrawl_batch_size = logic_config.get('recrawl_batch_size', 10)

    print("\n" + "=" * 60)
    print("ПАРСЕР СТАТЕЙ F1NEWS.RU")
    print("=" * 60)

    
    if not init_database(db_config):
        print("✗ Не удалось инициализировать БД")
        return

    
    conn = get_db_connection(db_config)
    if not conn:
        print("✗ Невозможно подключиться к базе данных!")
        return
    conn.close()
    print("✓ Подключение к БД установлено")

    
    saved_state = load_parser_state(db_config, source_name)
    existing_count = get_articles_count(db_config, source_name)

    print(f"✓ Статей в БД: {existing_count}")

    
    if saved_state and saved_state['current_date']:
        print(f" Найдено сохраненное состояние")
        print(f"  Последняя дата: {saved_state['current_date']}")

        resume = input("Продолжить с сохраненной даты? (y/n): ").strip().lower()
        if resume == 'y':
            try:
                start_date = datetime.strptime(saved_state['current_date'], "%Y/%m/%d")
            except:
                start_date_str = logic_config.get('start_date', '2025/12/22')
                start_date = datetime.strptime(start_date_str, "%Y/%m/%d")
        else:
            start_date_str = input("Введите стартовую дату (ГГГГ/ММ/ДД): ").strip()
            if not start_date_str:
                start_date_str = logic_config.get('start_date', '2025/12/22')
            start_date = datetime.strptime(start_date_str, "%Y/%m/%d")
    else:
        start_date_str = logic_config.get('start_date', '2025/12/22')
        start_date = datetime.strptime(start_date_str, "%Y/%m/%d")

    print("\n" + "=" * 60)
    print(f"ЦЕЛЬ: {target_count} статей")
    print(f"СТАРТ: {start_date.strftime('%Y/%m/%d')}")
    print(f"ЗАДЕРЖКА: {delay} сек")
    if recrawl_interval > 0:
        print(f"ПЕРЕОБКАЧКА: каждые {recrawl_interval} сек")
    print("=" * 60 + "\n")

    
    current_date = start_date
    articles_saved = existing_count
    days_without_articles = 0
    articles_processed_in_session = 0

    try:
        while articles_saved < target_count:
            date_str = date_to_str(current_date)

            
            if articles_processed_in_session > 0 and articles_processed_in_session % 10 == 0:
                save_parser_state(db_config, source_name, date_str, articles_saved)

            
            if days_without_articles >= max_empty_days:
                print(f"\nНет статей за {days_without_articles} дней. Останавливаемся.")
                break

            
            if recrawl_interval > 0 and articles_processed_in_session % 50 == 0:
                print(f"\n[ПЕРЕОБКАЧКА] Проверка старых статей...")
                urls_to_recrawl = get_urls_for_recrawl(db_config, source_name, recrawl_interval, recrawl_batch_size)
                if urls_to_recrawl:
                    print(f"  Найдено {len(urls_to_recrawl)} статей для проверки")
                    for url in urls_to_recrawl:
                        download_and_save_article(db_config, url, 0, delay, source_name, check_updates=True)

            print(f"\n[{date_str}] Поиск статей...")

            
            links = extract_article_links(base_url, date_str, delay)

            if not links:
                days_without_articles += 1
                print(f"  → Статей не найдено")
                current_date -= timedelta(days=1)
                continue

            days_without_articles = 0
            print(f"  → Найдено {len(links)} статей")

            
            for i, link in enumerate(links, 1):
                if articles_saved >= target_count:
                    break

                print(f"  [{i}/{len(links)}] ", end="")
                if download_and_save_article(db_config, link, articles_saved + 1, delay, source_name):
                    articles_saved += 1
                    articles_processed_in_session += 1

            
            current_date -= timedelta(days=1)

            
            if articles_saved % 100 == 0 and articles_saved > 0:
                print(f"\nПромежуточный итог: {articles_saved}/{target_count} статей")

        
        save_parser_state(db_config, source_name, date_to_str(current_date), articles_saved)

        
        print("\n" + "=" * 60)
        if articles_saved >= target_count:
            print("ЦЕЛЬ ДОСТИГНУТА!")
        else:
            print(" РАБОТА ЗАВЕРШЕНА")
        print(f"Всего статей в БД: {get_articles_count(db_config, source_name)}")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n\nПарсер остановлен пользователем")
        save_parser_state(db_config, source_name, date_to_str(current_date), articles_saved)
        print(f"Состояние сохранено. Собрано статей: {articles_saved}")
    except Exception as e:
        print(f"\n Критическая ошибка: {e}")
        save_parser_state(db_config, source_name, date_to_str(current_date), articles_saved)
        print(f"Состояние сохранено. Собрано статей: {articles_saved}")


if __name__ == "__main__":
    main()
