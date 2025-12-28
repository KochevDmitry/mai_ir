import requests
import os
from bs4 import BeautifulSoup
import time


def create_directory():
    if not os.path.exists("f1report_pages"):
        os.makedirs("f1report_pages")
        print("Создана папка 'f1report_pages'")
    else:
        print("Папка 'f1report_pages' уже существует")


def extract_links_from_file(filename):
    links = []

    try:
        with open(filename, 'r', encoding='utf-8') as file:
            content = file.read()

        lines = content.split('\n')
        for line in lines:
            line = line.strip()
            if line.startswith('<loc>') and line.endswith('</loc>'):
                url = line[5:-6].strip()  
                if url and url.startswith('http'):
                    links.append(url)

        print(f"Найдено {len(links)} ссылок")
        return links

    except Exception as e:
        print(f"Ошибка при чтении файла {filename}: {e}")
        return []


def save_page(url, index):
    try:
        time.sleep(1)

        print(f"Скачиваю {index}: {url}")

        response = requests.get(url)
        response.raise_for_status() 


        filename = url.split('/')[-1]

        filepath = os.path.join("f1report_pages", filename)

        with open(filepath, "wb") as file:
            file.write(response.content)

        print(f"  Сохранено: {filename} ({len(response.content)} байт)")
        return True

    except requests.exceptions.RequestException as e:
        print(f"  Ошибка при загрузке {url}: {e}")
        return False
    except Exception as e:
        print(f"  Неизвестная ошибка: {e}")
        return False


def main():
    """Основная функция"""
    input_file = "f1report_pages.txt"

    if not os.path.exists(input_file):
        print(f"Файл {input_file} не найден!")
        return

    create_directory()

    links = extract_links_from_file(input_file)

    if not links:
        print("Не найдено ссылок для скачивания")
        return

    print("\nНачинаю скачивание страниц...")
    print("-" * 50)

    success_count = 0
    for i, url in enumerate(links, 1):
        if save_page(url, i):
            success_count += 1

    print("-" * 50)
    print(f"\nГотово! Успешно сохранено {success_count} из {len(links)} страниц")
    print(f"Файлы находятся в папке: {os.path.abspath('f1report_pages')}")


if __name__ == "__main__":
    main()
