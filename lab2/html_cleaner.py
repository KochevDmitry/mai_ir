import psycopg2
from bs4 import BeautifulSoup
import os
import re
import xml.etree.ElementTree as ET
from xml.dom import minidom

DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_NAME = "f1news_db"
OUTPUT_XML = "articles.xml"  


def extract_f1news_content(html_content):
    """Извлечение контента из статей f1news.ru"""
    soup = BeautifulSoup(html_content, 'html.parser')

    content_div = soup.find('div', {'id': 'content'})
    if not content_div:
        content_div = soup.find('div', class_='post_content')

    if not content_div:
        return ""

    for unwanted in content_div.find_all(['div', 'script', 'style', 'ins', 'iframe']):
        unwanted.decompose()

    paragraphs = content_div.find_all('p')
    text = "\n".join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])

    return text


def extract_f1report_content(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')

    content_div = soup.find('div', {'id': 'content'})
    if not content_div:
        return ""

    for h1 in content_div.find_all('h1'):
        h1.decompose()

    textblock = content_div.find('div', {'id': 'textblock'})
    if textblock:
        content_div = textblock

    for unwanted in content_div.find_all(['div', 'script', 'style', 'ins', 'iframe', 'noindex']):
        if unwanted.name == 'div' and 'textblock' not in unwanted.get('id', ''):
            unwanted.decompose()

    text = content_div.get_text(separator='\n', strip=True)

    lines = [line.strip() for line in text.split('\n') if line.strip()]
    text = '\n'.join(lines)

    return text


def extract_content_by_source(html_content, url):
    if 'f1news.ru' in url:
        return extract_f1news_content(html_content)
    elif 'f1report.ru' in url:
        return extract_f1report_content(html_content)
    else:
        soup = BeautifulSoup(html_content, 'html.parser')

        for script in soup(["script", "style", "nav", "header", "footer", "aside"]):
            script.decompose()

        selectors = [
            'article',
            '.article',
            '.post-content',
            '.content',
            'main',
            '.main-content'
        ]

        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text(separator='\n', strip=True)

        body = soup.find('body')
        if body:
            return body.get_text(separator='\n', strip=True)

        return ""


def clean_text(text):
    text = re.sub(r'\s+', ' ', text)
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    return '\n'.join(lines)


def prettify_xml(elem):
    rough_string = ET.tostring(elem, encoding='utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")


def create_xml_structure():
    root = ET.Element("articles")
    root.set("xmlns", "http://www.example.com/f1news")
    root.set("version", "1.0")

    comment = ET.Comment(' F1 News Articles - Extracted from HTML content ')
    root.append(comment)

    return root


def save_articles_to_xml(articles_data, output_file=OUTPUT_XML):
    """Сохранение всех статей в XML файл"""

    
    root = create_xml_structure()

    
    meta = ET.SubElement(root, "meta")
    ET.SubElement(meta, "total_articles").text = str(len(articles_data))
    ET.SubElement(meta, "generated_date").text = "2024"

    
    for article in articles_data:
        article_id, text, url, source_name = article

        
        article_elem = ET.SubElement(root, "article")
        article_elem.set("id", str(article_id))
        article_elem.set("source", source_name)

        
        url_elem = ET.SubElement(article_elem, "url")
        url_elem.text = url

        
        text_elem = ET.SubElement(article_elem, "content")
        
        text_elem.text = f"<![CDATA[\n{text}\n]]>"

        
        stats_elem = ET.SubElement(article_elem, "statistics")
        ET.SubElement(stats_elem, "character_count").text = str(len(text))
        ET.SubElement(stats_elem, "word_count").text = str(len(text.split()))

    
    tree = ET.ElementTree(root)

    
    xml_str = prettify_xml(root)

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(xml_str)

    print(f"\nXML файл создан: {output_file}")
    print(f"Всего сохранено статей: {len(articles_data)}")

    return output_file


def process_articles_to_xml():
    """Основная функция для обработки статей из БД и сохранения в XML"""
    try:
        
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor()

        
        
        query = """
        SELECT id, html_content, url, source_name 
        FROM articles 
        WHERE html_content IS NOT NULL AND html_content != ''
        ORDER BY id
        """

        cursor.execute(query)
        articles = cursor.fetchall()

        print(f"Найдено {len(articles)} статей для обработки")

        
        extracted_articles = []

        for article in articles:
            article_id, html_content, url, source_name = article

            print(f"Обработка статьи ID: {article_id}, источник: {source_name}")

            
            text = extract_content_by_source(html_content, url)

            
            cleaned_text = clean_text(text)

            if cleaned_text:
                extracted_articles.append((article_id, cleaned_text, url, source_name))
                print(f"  Извлечено символов: {len(cleaned_text)}")
            else:
                print(f"  Предупреждение: не удалось извлечь текст для статьи {article_id}")
                

        cursor.close()
        conn.close()

        
        if extracted_articles:
            xml_file = save_articles_to_xml(extracted_articles)

            
            print("\n" + "=" * 50)
            print("СТАТИСТИКА:")
            print("=" * 50)

            total_chars = sum(len(article[1]) for article in extracted_articles)
            total_words = sum(len(article[1].split()) for article in extracted_articles)

            print(f"Всего статей: {len(extracted_articles)}")
            print(f"Общее количество символов: {total_chars}")
            print(f"Общее количество слов: {total_words}")
            print(f"Средняя длина статьи: {total_chars // len(extracted_articles)} символов")

            
            sources = {}
            for article in extracted_articles:
                source = article[3]
                sources[source] = sources.get(source, 0) + 1

            print(f"\nРаспределение по источникам:")
            for source, count in sources.items():
                print(f"  {source}: {count} статей")

        else:
            print("Не найдено статей для сохранения в XML")

    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()


def validate_xml(xml_file=OUTPUT_XML):
    """Проверка валидности созданного XML файла"""
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        print(f"\nПроверка XML файла: {xml_file}")
        print(f"Корневой элемент: {root.tag}")
        print(f"Количество статей в XML: {len(root.findall('article'))}")

        
        articles = root.findall('article')
        if articles:
            print(f"\nПример первой статьи:")
            first_article = articles[0]
            print(f"  ID: {first_article.get('id')}")
            print(f"  Источник: {first_article.get('source')}")
            content_elem = first_article.find('content')
            if content_elem is not None and content_elem.text:
                content_preview = content_elem.text[:100].replace('\n', ' ').replace('  ', ' ')
                print(f"  Текст (первые 100 символов): {content_preview}...")

        return True

    except Exception as e:
        print(f"Ошибка при проверке XML: {e}")
        return False


if __name__ == "__main__":
    
    process_articles_to_xml()

    
    if os.path.exists(OUTPUT_XML):
        validate_xml()

    print("\nГотово! Все статьи сохранены в единый XML файл.")
