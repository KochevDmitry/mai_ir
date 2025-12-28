import os
import time
from multiprocessing import Pool, cpu_count
from selectolax.lexbor import LexborHTMLParser

def clean_html_fast(html_content):
    tree = LexborHTMLParser(html_content)
    for tag in tree.css('script, style, nav, header, footer, iframe'):
        tag.remove()

    text = tree.body.text(separator=' ') if tree.body else ""
    return " ".join(text.split())


def process_file(args):
    file_path, encoding = args
    try:
        raw_size = os.path.getsize(file_path)
        with open(file_path, 'r', encoding=encoding, errors='replace') as f:
            content = f.read()
            clean_text = clean_html_fast(content)
            text_size = len(clean_text.encode('utf-8'))
            return raw_size, text_size, 1
    except Exception:
        return 0, 0, 0


def run_analysis(folder_path, source_name, encoding):
    print(f"\n>>> Сканирование папки: {source_name}...")

    files_to_process = []
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith((".html", ".htm")):
                files_to_process.append((os.path.join(root, file), encoding))

    total_files = len(files_to_process)
    if total_files == 0:
        print("Файлы не найдены.")
        return None

    print(f"Найдено: {total_files} файлов. Запуск в {cpu_count()} потоках...")

    total_raw = 0
    total_text = 0
    processed_count = 0
    start_time = time.time()

    with Pool(processes=cpu_count()) as pool:
        for raw, txt, count in pool.imap_unordered(process_file, files_to_process, chunksize=100):
            total_raw += raw
            total_text += txt
            processed_count += count

            if processed_count % 500 == 0 or processed_count == total_files:
                elapsed = time.time() - start_time
                speed = processed_count / elapsed
                remaining = total_files - processed_count
                eta = remaining / speed if speed > 0 else 0
                print(f"[{source_name}] Готово: {processed_count}/{total_files} | "
                      f"Скорость: {speed:.1f} док/сек | Осталось: {eta:.1f} сек.")

    return {
        "Источник": source_name,
        "Кол-во документов": processed_count,
        "Общий размер (сырой), МБ": total_raw / (1024 * 1024),
        "Общий размер (текст), МБ": total_text / (1024 * 1024),
        "Средний размер текста, КБ": (total_text / processed_count) / 1024 if processed_count > 0 else 0
    }


if __name__ == "__main__":
    configs = [
        ("../../pages_DO_NOT_TOUCH", "f1news.ru", "utf-8"),
        ("../../f1report_pages", "f1report.ru", "windows-1251")
    ]

    all_results = []
    overall_start = time.time()

    for path, name, enc in configs:
        if os.path.exists(path):
            res = run_analysis(path, name, enc)
            if res:
                all_results.append(res)
        else:
            print(f"Путь {path} не существует!")

    print("\n" + "=" * 90)
    print(f"{'Источник':<15} | {'Кол-во':<8} | {'Сырой(МБ)':<12} | {'Текст(МБ)':<12} | {'Ср.текст(КБ)':<12}")
    print("-" * 90)
    for r in all_results:
        print(f"{r['Источник']:<15} | {r['Кол-во документов']:<8} | "
              f"{r['Общий размер (сырой), МБ']:<12.2f} | {r['Общий размер (текст), МБ']:<12.2f} | "
              f"{r['Средний размер текста, КБ']:<12.2f}")
    print("=" * 90)
    print(f"Общее время выполнения: {time.time() - overall_start:.1f} сек.")
