import requests
import os
import time
import threading
from datetime import datetime, timedelta
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
import sys
import signal

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('f1news_parser.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class StrictRateLimiter:

    def __init__(self, rate=1.0):

        self.rate = rate
        self.min_interval = 1.0 / rate  
        self.last_request_time = 0
        self.lock = threading.Lock()
        logger.info(f"Rate limiter инициализирован: {rate} запросов в секунду")

    def wait(self):
        with self.lock:
            current_time = time.time()

            if self.last_request_time > 0:
                elapsed = current_time - self.last_request_time

                if elapsed < self.min_interval:
                    sleep_time = self.min_interval - elapsed
                    logger.debug(f"Rate limiting: ожидание {sleep_time:.3f} сек")
                    time.sleep(sleep_time)
                    current_time = time.time() 

            self.last_request_time = current_time

    def get_stats(self):
        with self.lock:
            return {
                'rate': self.rate,
                'min_interval': self.min_interval,
                'last_request_time': self.last_request_time,
                'time_since_last': time.time() - self.last_request_time if self.last_request_time > 0 else 0
            }


class ProgressMonitor:

    def __init__(self, parser_instance):
        self.parser = parser_instance
        self.running = True
        self.thread = None
        self.start_time = time.time()

    def display_progress(self):
        while self.running:
            try:
                count = self.parser.articles_count
                target = self.parser.target_count
                percentage = (count / target * 100) if target > 0 else 0

                elapsed = time.time() - self.start_time
                if count > 0:
                    articles_per_hour = (count / elapsed) * 3600
                    estimated_total = (target - count) / (count / elapsed) if count > 0 else 0
                    eta_str = str(timedelta(seconds=int(estimated_total))) if estimated_total > 0 else "N/A"
                else:
                    articles_per_hour = 0
                    eta_str = "N/A"

                sys.stdout.write(f"\rПрогресс: {count}/{target} статей ({percentage:.2f}%) | "
                                 f"Скорость: {articles_per_hour:.1f}/час | "
                                 f"ETA: {eta_str} | "
                                 f"Дата: {self.parser.current_date_str if hasattr(self.parser, 'current_date_str') else 'N/A'}")
                sys.stdout.flush()

                if count >= target:
                    print(f"\n\n🎉 Цель достигнута! Собрано {count} статей.")
                    self.running = False
                    break

                time.sleep(1)  
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Ошибка в мониторе прогресса: {e}")
                time.sleep(5)

    def start(self):
        self.thread = threading.Thread(target=self.display_progress, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=2)
        print()  

class F1NewsParser:
    def __init__(self, base_url="https://www.f1news.ru"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'F1NewsParser/1.0 (educational project; contact@example.com)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
        })

        self.rate_limiter = StrictRateLimiter(rate=2.0) 

        self.pages_dir = "pages"
        os.makedirs(self.pages_dir, exist_ok=True)

        self.articles_count = 0
        self.target_count = 30000
        self.delay = 1  
        self.current_date_str = ""

        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0

        self.running = True
        self.monitor = ProgressMonitor(self)

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        logger.info(f"Инициализирован парсер. Статьи будут сохранены в: {os.path.abspath(self.pages_dir)}")

    def signal_handler(self, signum, frame):
        """Обработчик сигналов для корректного завершения"""
        logger.info(f"\nПолучен сигнал {signum}. Завершаем работу...")
        self.running = False
        self.monitor.stop()
        self.print_statistics()
        sys.exit(0)

    def make_rate_limited_request(self, url, method='GET', timeout=10, **kwargs):
        if not self.running:
            return None

        self.total_requests += 1

        try:
            self.rate_limiter.wait()

            logger.debug(f"Запрос #{self.total_requests}: {url[:80]}...")

            response = self.session.request(method, url, timeout=timeout, **kwargs)

            if response.status_code == 200:
                self.successful_requests += 1
            else:
                self.failed_requests += 1
                logger.warning(f"HTTP {response.status_code} для {url}")

            return response

        except requests.exceptions.Timeout:
            self.failed_requests += 1
            logger.warning(f"Таймаут запроса: {url}")
            return None
        except requests.exceptions.RequestException as e:
            self.failed_requests += 1
            logger.warning(f"Ошибка запроса {url}: {e}")
            return None
        except Exception as e:
            self.failed_requests += 1
            logger.error(f"Неожиданная ошибка при запросе {url}: {e}")
            return None

    def parse_date_articles(self, date_str):
        """Парсит все статьи за определенную дату"""
        url = f"{self.base_url}/news/{date_str}/"

        try:
            logger.debug(f"Загружаем страницу даты: {url}")
            response = self.make_rate_limited_request(url, timeout=15)

            if response is None:
                return []

            if response.status_code == 404:
                logger.debug(f"Страница {url} не найдена (404)")
                return []

            soup = BeautifulSoup(response.content, 'html.parser')

            articles = set()

            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link['href']
                if '/news/' in href and date_str.replace('/', '/') in href:
                    full_url = urljoin(self.base_url, href)
                    articles.add(full_url)

            news_items = soup.find_all(['div', 'article', 'li'], class_=re.compile(r'news|article|item', re.I))
            for item in news_items:
                links = item.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    if '/news/' in href:
                        full_url = urljoin(self.base_url, href)
                        articles.add(full_url)

            articles_list = list(articles)
            logger.info(f"Найдено {len(articles_list)} статей за {date_str}")

            return articles_list

        except Exception as e:
            logger.error(f"Неожиданная ошибка при парсинге {url}: {e}")
            return []

    def save_article(self, url):
        """Сохраняет HTML страницу статьи"""
        if not self.running:
            return False

        try:
            if self.articles_count >= self.target_count:
                return False

            logger.debug(f"Загрузка статьи: {url}")

            response = self.make_rate_limited_request(url, timeout=20)

            if response is None or response.status_code != 200:
                return False

           
            url_path = url.replace(self.base_url, '').strip('/')
            if not url_path:
                url_path = f"article_{self.articles_count + 1}"

            filename = url_path.replace('/', '_')
            if not filename.endswith('.html'):
                filename += '.html'

            filename = re.sub(r'[<>:"\\|?*]', '_', filename)

            if len(filename) > 100:
                name, ext = os.path.splitext(filename)
                filename = name[:95] + ext

            filepath = os.path.join(self.pages_dir, filename)

            if os.path.exists(filepath):
                logger.debug(f"Статья уже существует: {filename}")
                self.articles_count += 1
                return True

            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(response.text)

            self.articles_count += 1

            self.save_metadata(url, filename)

            logger.info(f"✓ Сохранена статья #{self.articles_count}: {filename}")

            return True

        except Exception as e:
            logger.error(f"Неожиданная ошибка при сохранении {url}: {e}")
            return False

    def save_metadata(self, url, filename):
        """Сохраняет метаданные статьи в отдельный файл"""
        metadata_file = os.path.join(self.pages_dir, "metadata.csv")

        try:
            if not os.path.exists(metadata_file):
                with open(metadata_file, 'w', encoding='utf-8') as f:
                    f.write("id,url,filename,date_downloaded,date_published\n")

            date_published = "unknown"
            match = re.search(r'news/(\d{4}/\d{2}/\d{2})/', url)
            if match:
                date_published = match.group(1)

            with open(metadata_file, 'a', encoding='utf-8') as f:
                f.write(f"{self.articles_count},{url},{filename},"
                        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')},{date_published}\n")
        except Exception as e:
            logger.error(f"Ошибка при сохранении метаданных: {e}")

    def date_to_str(self, date_obj):
        """Преобразует объект datetime в строку формата YYYY/MM/DD"""
        return date_obj.strftime("%Y/%m/%d")

    def str_to_date(self, date_str):
        """Преобразует строку в объект datetime"""
        try:
            return datetime.strptime(date_str, "%Y/%m/%d")
        except ValueError:
            for fmt in ["%Y-%m-%d", "%Y.%m.%d", "%d/%m/%Y", "%d.%m.%Y"]:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            raise ValueError(f"Неверный формат даты: {date_str}")

    def get_existing_articles_count(self):
        if not os.path.exists(self.pages_dir):
            return 0

        html_files = [f for f in os.listdir(self.pages_dir) if f.endswith('.html')]
        return len(html_files)

    def get_last_processed_date(self):
        metadata_file = os.path.join(self.pages_dir, "metadata.csv")

        if not os.path.exists(metadata_file):
            return None

        try:
            with open(metadata_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                if len(lines) > 1:  
                    last_line = lines[-1].strip()
                    parts = last_line.split(',')
                    if len(parts) >= 5 and parts[4] != 'unknown' and parts[4] != 'date_published':
                        date_str = parts[4].replace('/', '-')
                        try:
                            return datetime.strptime(date_str, "%Y-%m-%d")
                        except:
                            return None
        except Exception as e:
            logger.error(f"Ошибка при чтении метаданных: {e}")

        return None

    def print_statistics(self):
        """Выводит статистику работы парсера"""
        success_rate = (self.successful_requests / self.total_requests * 100) if self.total_requests > 0 else 0

        print("\n" + "=" * 70)
        print("СТАТИСТИКА РАБОТЫ ПАРСЕРА")
        print("=" * 70)
        print(f"Всего статей собрано: {self.articles_count}")
        print(f"Целевое количество: {self.target_count}")
        print(f"Всего запросов: {self.total_requests}")
        print(f"Успешных запросов: {self.successful_requests} ({success_rate:.1f}%)")
        print(f"Неудачных запросов: {self.failed_requests}")

        stats = self.rate_limiter.get_stats()
        print(f"\nСТАТИСТИКА RATE LIMITER:")
        print(f"  Запросов в секунду: {stats['rate']}")
        print(f"  Минимальный интервал: {stats['min_interval']:.3f} сек")
        print(f"  Время с последнего запроса: {stats['time_since_last']:.3f} сек")

        print("\nСОВЕТЫ:")
        if success_rate < 90:
            print("   Много неудачных запросов. Проверьте подключение к интернету.")
        if self.articles_count < self.target_count * 0.1:
            print("   Собрано мало статей. Проверьте стартовую дату.")
        print("=" * 70)

    def run(self, start_date_str=None):

        existing_count = self.get_existing_articles_count()
        self.articles_count = existing_count

        if existing_count > 0:
            print(f"Найдено {existing_count} уже скачанных статей.")
            resume = input("Продолжить с места остановки? (y/n): ").strip().lower()

            if resume == 'y':
                last_date = self.get_last_processed_date()
                if last_date:
                    print(f"Продолжаем с даты: {last_date.strftime('%Y/%m/%d')}")
                    start_date = last_date
                else:
                    print("Не удалось определить последнюю дату. Начинаем заново.")
                    start_date_str = input("Введите стартовую дату (ГГГГ/ММ/ДД): ").strip()
                    if not start_date_str:
                        start_date_str = "2025/12/22"
                    start_date = self.str_to_date(start_date_str)
            else:
                start_date_str = input("Введите стартовую дату (ГГГГ/ММ/ДД): ").strip()
                if not start_date_str:
                    start_date_str = "2025/12/22"
                start_date = self.str_to_date(start_date_str)
        else:
            if not start_date_str:
                start_date_str = input("Введите стартовую дату (ГГГГ/ММ/ДД, по умолчанию 2025/12/22): ").strip()
                if not start_date_str:
                    start_date_str = "2025/12/22"

            try:
                start_date = self.str_to_date(start_date_str)
            except ValueError as e:
                print(f"Ошибка: {e}")
                start_date_str = "2025/12/22"
                start_date = self.str_to_date(start_date_str)

        logger.info("=" * 60)
        logger.info(f"НАЧАЛО РАБОТЫ ПАРСЕРА F1NEWS")
        logger.info(f"Цель: собрать {self.target_count} статей")
        logger.info(f"Уже собрано: {existing_count} статей")
        logger.info(f"Осталось собрать: {self.target_count - existing_count} статей")
        logger.info(f"Стартовая дата: {start_date.strftime('%Y/%m/%d')}")
        logger.info(f"Rate limiting: 1 запрос в секунду")
        logger.info("=" * 60)

        print(f"\n{'=' * 60}")
        print(f"ЦЕЛЬ: {self.target_count} статей")
        print(f"СТАРТ: {start_date.strftime('%Y/%m/%d')}")
        print(f"RATE LIMITING: 1 запрос в секунду")
        print(f"ЗАДЕРЖКА МЕЖДУ ДАТАМИ: {self.delay} сек")
        print(f"{'=' * 60}\n")

        self.monitor.start()

        current_date = start_date
        days_without_articles = 0
        consecutive_empty_days_limit = 30  
        try:
            while self.articles_count < self.target_count and self.running:
                self.current_date_str = self.date_to_str(current_date)

                if days_without_articles >= consecutive_empty_days_limit:
                    logger.warning(f"Не найдено статей за последние {days_without_articles} дней. Останавливаемся.")
                    print(f"\n Не найдено статей за {days_without_articles} дней. Возможно, достигли конца архива.")
                    break

                articles = self.parse_date_articles(self.current_date_str)

                if articles:
                    days_without_articles = 0 
                    logger.info(f"Обработка {len(articles)} статей за {self.current_date_str}")

                    for i, article_url in enumerate(articles, 1):
                        if not self.running or self.articles_count >= self.target_count:
                            break

                        logger.debug(f"Статья {i}/{len(articles)}: {article_url}")
                        self.save_article(article_url)


                else:
                    days_without_articles += 1
                    logger.debug(f"Статей за {self.current_date_str} не найдено")

                    if days_without_articles % 10 == 0:
                        logger.warning(f"Не найдено статей за последние {days_without_articles} дней")

                current_date -= timedelta(days=1)

                time.sleep(self.delay)

                if self.articles_count % 100 == 0 and self.articles_count > 0:
                    logger.info(f"Промежуточный итог: {self.articles_count}/{self.target_count} статей")
                    self.print_statistics()

            self.monitor.stop()
            self.print_statistics()

            if self.articles_count >= self.target_count:
                logger.info("=" * 60)
                logger.info(f" ЦЕЛЬ ДОСТИГНУТА!")
                logger.info(f"Всего собрано: {self.articles_count} статей")
                logger.info(f"Статьи сохранены в: {os.path.abspath(self.pages_dir)}")
                logger.info("=" * 60)

                print("\n" + "=" * 60)
                print(f"УСПЕХ Собрано {self.articles_count} статей!")
                print(f" Папка с результатами: {os.path.abspath(self.pages_dir)}")
                print(f" Метаданные: {os.path.join(self.pages_dir, 'metadata.csv')}")
                print("=" * 60)
            else:
                logger.info(f"Работа завершена. Собрано {self.articles_count} статей")
                print(f"\n\nРабота завершена. Собрано {self.articles_count} статей.")

        except KeyboardInterrupt:
            self.monitor.stop()
            self.print_statistics()
            logger.info("Парсер остановлен пользователем")
            print(f"\n\n⏹ Парсер остановлен. Собрано {self.articles_count} статей.")
        except Exception as e:
            self.monitor.stop()
            self.print_statistics()
            logger.error(f"Критическая ошибка: {e}", exc_info=True)
            print(f"\n\nКритическая ошибка: {e}")
            print(f"Собрано {self.articles_count} статей.")


def main():
    try:
        try:
            import requests
            import bs4
        except ImportError as e:
            print(f" Ошибка: Не установлены необходимые библиотеки")
            return

        parser = F1NewsParser()

        if len(sys.argv) > 1:
            start_date = sys.argv[1]
            parser.run(start_date)
        else:
            parser.run()

    except Exception as e:
        print(f"Непредвиденная ошибка: {e}")
        logger.error(f"Непредвиденная ошибка: {e}", exc_info=True)


if __name__ == "__main__":
    main()
