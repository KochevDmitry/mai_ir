
"""
Парсер статей F1Report.ru
Собирает HTML страницы статей из sitemap и сохраняет в PostgreSQL
"""

import requests
import time
from datetime import datetime
from bs4 import BeautifulSoup
import psycopg2
import yaml
import sys
import hashlib
import signal


class GracefulKiller:
    """Обработчик сигналов для корректной остановки"""
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        print("\n⏹ Получен сигнал остановки...")
        self.kill_now = True


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
                last_processed_url TEXT,
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
    """Вычисляет MD5 хеш контента"""
    return hashlib.md5(html_content.encode('utf-8')).hexdigest()


def save_parser_state(db_config, source_name, last_url, articles_count):
    """Сохраняет состояние парсера"""
    conn = get_db_connection(db_config)
    if not conn:
        return False

    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO parser_state (source_name, last_processed_url, articles_count, last_update)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (source_name) 
            DO UPDATE SET 
                last_processed_url = EXCLUDED.last_processed_url,
                articles_count = EXCLUDED.articles_count,
                last_update = EXCLUDED.last_update
        ''', (source_name, last_url, articles_count, int(time.time())))

        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"  ✗ Ошибка сохранения состояния: {e}")
        return False


def load_parser_state(db_config, source_name):
    """Загружает состояние парсера"""
    conn = get_db_connection(db_config)
    if not conn:
        return None

    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT last_processed_url, articles_count 
            FROM parser_state 
            WHERE source_name = %s
        ''', (source_name,))

        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if result:
            return {'last_url': result[0], 'articles_count': result[1]}
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
        print(f"✗ Ошибка при подсчете: {e}")
        return 0


def get_article_hash(db_config, url):
    """Получает хеш статьи"""
    conn = get_db_connection(db_config)
    if not conn:
        return None

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT content_hash FROM article_hashes WHERE url = %s", (url,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result[0] if result else None
    except Exception as e:
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
        return False


def article_exists(db_config, url):
    """Проверяет существование статьи"""
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
        return False


def save_article_to_db(db_config, url, html_content, article_number, source_name='f1report.ru'):
    """Сохраняет статью в БД"""
    conn = get_db_connection(db_config)
    if not conn:
        return False

    try:
        cursor = conn.cursor()

        
        
        filename = url.split('/')[-1]
        if not filename:
            filename = f"article_{article_number}.html"

        current_time = int(time.time())
        crawl_date_str = datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S')

        
        cursor.execute('''
            INSERT INTO articles (url, html_content, source_name, crawl_timestamp, crawl_date, filename, article_number)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) 
            DO UPDATE SET 
                html_content = EXCLUDED.html_content,
                crawl_timestamp = EXCLUDED.crawl_timestamp,
                crawl_date = EXCLUDED.crawl_date
        ''', (
            url,
            html_content,
            source_name,
            current_time,
            crawl_date_str,
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


def extract_links_from_sitemap(sitemap_url, delay):
    """Извлекает ссылки из sitemap"""
    try:
        time.sleep(delay)

        print(f"Загружаю sitemap: {sitemap_url}")
        response = requests.get(sitemap_url, timeout=30)

        if response.status_code != 200:
            print(f"  ✗ HTTP {response.status_code}")
            return []

        
        links = []
        lines = response.text.split('\n')
        for line in lines:
            line = line.strip()
            if line.startswith('<loc>') and line.endswith('</loc>'):
                url = line[5:-6].strip()  
                if url and url.startswith('http'):
                    links.append(url)

        print(f"✓ Найдено {len(links)} ссылок в sitemap")
        return links

    except Exception as e:
        print(f"  ✗ Ошибка загрузки sitemap: {e}")
        return []


def download_and_save_article(db_config, url, article_number, delay, source_name='f1report.ru', check_updates=False):
    """Скачивает и сохраняет статью (с декодированием windows-1251)"""
    try:
        time.sleep(delay)

        response = requests.get(url, timeout=20)

        if response.status_code != 200:
            print(f"  ✗ HTTP {response.status_code} для {url}")
            return False

        
        try:
            html_bytes = response.content
            html_text = html_bytes.decode('windows-1251', errors='replace')
        except:
            
            html_text = response.text

        
        html_text = html_text.replace('charset=windows-1251', 'charset=UTF-8')
        html_text = html_text.replace('charset=Windows-1251', 'charset=UTF-8')

        
        if '<meta charset=' not in html_text[:500].lower():
            html_text = html_text.replace('<head>', '<head>\n<meta charset="UTF-8">', 1)

        new_hash = get_content_hash(html_text)

        
        exists = article_exists(db_config, url)

        if exists:
            if check_updates:
                old_hash = get_article_hash(db_config, url)
                if old_hash and old_hash != new_hash:
                    if save_article_to_db(db_config, url, html_text, article_number, source_name):
                        return True
                else:
                    print(f"  → Без изменений: {url}")
                    return False
            else:
                print(f"  → Уже в БД: {url}")
                return True
        else:
            if save_article_to_db(db_config, url, html_text, article_number, source_name):
                return True

        return False

    except requests.exceptions.RequestException as e:
        print(f"  ✗ Ошибка загрузки {url}: {e}")
        return False
    except Exception as e:
        print(f"  ✗ Неизвестная ошибка: {e}")
        return False


def get_urls_for_recrawl(db_config, source_name, recrawl_interval, limit=10):
    """Получает URL для переобкачки"""
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
        return []


def main():
    """Основная функция"""

    if len(sys.argv) < 2:
        print("Использование: python f1report_parser.py <путь_к_конфигу.yaml>")
        sys.exit(1)

    config_path = sys.argv[1]
    config = load_config(config_path)

    db_config = config['db']
    logic_config = config['logic']

    sitemap_url = logic_config.get('sitemap_url', 'https://f1report.ru/sitemap.xml')
    source_name = logic_config.get('source_name', 'f1report.ru')
    delay = logic_config.get('delay', 1.0)
    target_count = logic_config.get('target_count', 10000)
    recrawl_interval = logic_config.get('recrawl_interval', 0)
    recrawl_batch_size = logic_config.get('recrawl_batch_size', 10)

    killer = GracefulKiller()

    print("\n" + "=" * 60)
    print("ПАРСЕР СТАТЕЙ F1REPORT.RU")
    print("=" * 60)

    if not init_database(db_config):
        return

    conn = get_db_connection(db_config)
    if not conn:
        return
    conn.close()
    print("✓ Подключение к БД установлено")

    saved_state = load_parser_state(db_config, source_name)
    existing_count = get_articles_count(db_config, source_name)

    print(f"✓ Статей в БД: {existing_count}")

    
    links = extract_links_from_sitemap(sitemap_url, delay)

    if not links:
        print("✗ Не удалось загрузить ссылки из sitemap")
        return

    
    start_index = 0
    if saved_state and saved_state['last_url']:
        try:
            start_index = links.index(saved_state['last_url']) + 1
            print(f"✓ Продолжаю с позиции {start_index}")
        except ValueError:
            print("✓ Начинаю с начала списка")

    print("\n" + "=" * 60)
    print(f"ЦЕЛЬ: {target_count} статей")
    print(f"ВСЕГО ССЫЛОК: {len(links)}")
    print(f"СТАРТ С: {start_index}")
    print(f"ЗАДЕРЖКА: {delay} сек")
    print("=" * 60 + "\n")

    articles_saved = existing_count
    articles_processed = 0

    try:
        for i in range(start_index, len(links)):
            if killer.kill_now or articles_saved >= target_count:
                break

            url = links[i]

            
            if articles_processed > 0 and articles_processed % 10 == 0:
                save_parser_state(db_config, source_name, url, articles_saved)

            
            if recrawl_interval > 0 and articles_processed % 50 == 0 and articles_processed > 0:
                print(f"\n[ПЕРЕОБКАЧКА] Проверка старых статей...")
                urls_to_recrawl = get_urls_for_recrawl(db_config, source_name, recrawl_interval, recrawl_batch_size)
                if urls_to_recrawl:
                    print(f"  Найдено {len(urls_to_recrawl)} статей для проверки")
                    for recrawl_url in urls_to_recrawl:
                        download_and_save_article(db_config, recrawl_url, 0, delay, source_name, check_updates=True)
                print()

            print(f"[{i + 1}/{len(links)}] ", end="")
            if download_and_save_article(db_config, url, articles_saved + 1, delay, source_name):
                articles_saved += 1

            articles_processed += 1

            
            if articles_processed % 100 == 0:
                print(f"\n📊 Обработано: {articles_processed}, Сохранено: {articles_saved}/{target_count}\n")

        
        if i < len(links) - 1:
            save_parser_state(db_config, source_name, links[i], articles_saved)

        print("\n" + "=" * 60)
        if articles_saved >= target_count:
            print("🎉 ЦЕЛЬ ДОСТИГНУТА!")
        else:
            print("🏁 РАБОТА ЗАВЕРШЕНА")
        print(f"Всего статей в БД: {get_articles_count(db_config, source_name)}")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n\n⏹ Парсер остановлен")
        if i < len(links):
            save_parser_state(db_config, source_name, links[i], articles_saved)
        print(f"Состояние сохранено. Собрано: {articles_saved}")
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        if i < len(links):
            save_parser_state(db_config, source_name, links[i], articles_saved)


if __name__ == "__main__":
    main()
