import os
import pandas as pd
import sqlite3

import fitz
import docx
from docx import Document
import whisper # Для аудио формата
import time
import datetime

def is_file_accessible(path: str) -> bool:
    """
    Проверяет реальную возможность чтения файла. Для этого пытаемся получить
    право писать и читать файл
    """
    try:
        # Пытаемся открыть файл на чтение. 
        # Используем бинарный режим 'rb', чтобы не возиться с кодировками.
        with open(path, 'rb'):
            pass
        return True
    except (PermissionError, OSError):
        return False

def forming_table(root_dir: str = ".//") -> pd.DataFrame:
    """
    функция forming_table принимает на вход ссылку на корневую папку (условная база данных) и возвращает
    таблицу с метаданными о каждом файле (название, расширение, путь). также создает дополнительно колонки
    оценки опасности, содержания, категории.
    """

    raw_data = []

    if not os.path.exists(root_dir):
        print(f"Ошибка: такой директории {root_dir} нет на устройстве")
    else:

        for root, dirs, files in os.walk(root_dir): # os.walk возвращает путь до нынешней папки, все папки в наличии, все файлы в наличии

            for name in files:

                try:

                    full_path = os.path.join(root, name) # полный путь = склейка пути до текущей папки и имени файла с расширением

                    if not is_file_accessible(full_path):

                        raw_data.append({
                            "Имя файла": os.path.splitext(name)[0].lower(),
                            "Путь": full_path,
                            "Расширение": os.path.splitext(name)[1].lower(),
                            "Дата создания": "НЕТ ДОСТУПА",
                            "Содержание": "НЕТ ДОСТУПА"
                        })
                        continue 

                    ext = os.path.splitext(name)[1].lower() # ext[0] - имя файла, ext[1] - расширение
                    name = os.path.splitext(name)[0].lower()
                    date = os.path.getctime(full_path)
                    date = datetime.datetime.fromtimestamp(date).strftime('%Y-%m-%d %H:%M:%S')
                    
                    file_info = {
                                "Имя файла" : name,
                                "Путь" : full_path,
                                "Расширение": ext,
                                "Дата создания": date
                                }
                    
                    raw_data.append(file_info)
                
                except Exception as e:

                    print(f"Ошбика при обработке метаданных файла {name}: {e}")
                    # ловим другие ошибки (файл удален в процессе поиска и т.д.)
                df = pd.DataFrame(raw_data)
                df['Содержание'] = "NO"
                df['Рейтинг опасности'] = 0.0
                df['Найденные ПДн'] = "NO"

    return df

def parsing(df: pd.DataFrame) -> None:
    """
    функция parsing принимает на вход таблицу в pandas и возвращает измененный датафрейм.
    в функции парсинг есть вложенная служебная функция choose_engine, принимающая
    на вход строку (расширение файла) и возвращающая в виде строки тип используемого парсера.
    цикл итеративно перебирает строки таблицы, берет расширение и ссылку на файл, определяет 
    тип парсера и извлекает текст в соответствующую ячейку содержание.
    """

    def choose_engine(extension: str) -> str:

        extension = extension.lower()
        cases = {
            # Группа 1: PDF (через PyMuPDF/fitz)
            ".pdf": "pdf_engine",

            # Группа 2: Word (через python-docx)
            ".docx": "docx_engine",

            # Группа 3: Текст (через встроенный open)
            ".txt": "text_engine", ".log": "text_engine", ".md": "text_engine", 
            ".xml": "text_engine", ".html": "text_engine", ".htm": "text_engine",

            # Группа 4: Аудио (через Whisper)
            ".mp3": "whisper", ".wav": "whisper", ".m4a": "whisper",
            ".flac": "whisper", ".ogg": "whisper"
        }
        return cases.get(extension, "skip")
    
    audio_model = whisper.load_model("base")

    for idx, row in df.iterrows():

        path = row["Путь"]
        path = os.path.normpath(path=path)
        ext = row["Расширение"]
        engine = choose_engine(ext)

        if not is_file_accessible(path) or os.path.getsize(path) == 0:

            df.at[idx, "Содержание"] = "ПУСТОЙ ФАЙЛ" if os.path.getsize(path) == 0 else "НЕТ ДОСТУПА"
            continue

        if engine == "pdf_engine":
            try:
                with fitz.open(path) as doc:

                    text = ""
                    for page in doc:

                        text += page.get_text()
                    df.at[idx, "Содержание"] = text.strip() if text else "ПУСТОЙ ПДФ"

            except Exception as e:
                print(f"Ошибка в чтении файла PDF: {e}")
                df.at[idx, "Содержание"] = f"Ошибка в чтении файла PDF: {e}"
        
        elif engine == "docx_engine":
            try:
                doc = Document(path)
                text = "\n".join([p.text for p in doc.paragraphs])
                df.at[idx, "Содержание"] = text.strip() if text else "ПУСТОЙ DOCX"
            except Exception as e:
                print(f"Ошибка в чтении файла DOCx: {e}")
                df.at[idx, "Содержание"] = f"Ошибка в чтении файла DOCx: {e}"
        
        elif engine == "text_engine":
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as txt:
                    df.at[idx, "Содержание"] = txt.read().strip()
            except Exception as e:
                print(f"Ошибка в чтении файла текстового формата: {e}")
                df.at[idx, "Содержание"] = f"Ошибка в чтении файла текстового формата: {e}"

        elif engine == "whisper":

            try:
                audio = whisper.load_audio(path)
                audio_segment = whisper.pad_or_trim(audio)

                mel = whisper.log_mel_spectrogram(audio_segment).to(audio_model.device)
                _, probs = audio_model.detect_language(mel)

                detected_language = max(probs, key=probs.get)

                result = audio_model.transcribe(path, language = detected_language)

                df.at[idx, "Содержание"] = result["text"].strip() if result else "НИЧЕГО НЕ ИЗВЛЕЧЕНО"
            
            except Exception as e:

                print(f"Произошел сбой при извлечении аудиодорожки: {e}")
                df.at[idx, "Содержание"] = f"Произошел сбой при извлечении аудиодорожки: {e}"
    
    return df

def seek_danger(df: pd.DataFrame) -> pd.DataFrame:
    """
    функция seek_danger принимает на вход датафрейм с извлеченным текстом
    и возвращает датафрейм с измененной колонкой "найденные пдн". в эту колонку
    записываем все, что совпало с ключами (согласно фз №152).
    все текстовые данные внутри функции переводятся в нижний регистр для
    обеспечения точности сопоставления паттернов.
    """

    # Поиск подстроки в строке бессмысленен, так как заведомо неизвестен набор символов
    # Значит необходимо искать по паттернам (чтобы подстрока удовлетворила паттерну)
    # \d - цифра
    # {2} {6} - квантификаторы (сколько цифр подряд)
    # \s - пробел
    # ? - квантор. Если поставить после \s это делает символ пробела не обязательным
    patterns = {

        # Тип 1 - простая текстовая информация
        "Паспорт": r"\d{2}\s?\d{2}\s?\d{6}",
        "СНИЛС": r"\d{3}-\d{3}-\d{3}\s\d{2}",
        "Телефон": r"(?:\+7|8)[\s\(-]*\d{3}[\s\)-]*\d{3}[\s-]*\d{2}[\s-]*\d{2}",
        "ИНН": r"\b\d{10}\b|\b\d{12}\b",
        "Email": r"[\w\.-]+@[\w\.-]+\.\w+",

        # Тип 2 - биометрические
        "Биометрия: лицо": "лицо",
        "Биометрия: глаза": "глаза",
        "Биометрия: отпечатки": "отпечатки",

        # Тип 3 - медицинские документы
        "Медицина: Диагноз/Анамнез": r"(?i)\b(?:диагноз|анамнез|жалобы|лечение|терапия|мкб-\d+)\b",
        "Медицина: Полис ОМС": r"\b\d{16}\b",  # Единый номер полиса ОМС состоит из 16 цифр
        "Медицина: Рецепт/Препараты": r"(?i)\b(?:рецепт|назначено|мг/сут|таблетки|дозировка)\b",
        "Медицина: Медучреждение": r"(?i)\b(?:больница|поликлиника|медцентр|клиника|врач-[\w]+)\b"

    }

    for idx, row in df.iterrows():

        if row["Содержание"].split(" ")[0] == "Ошибка":

            df.at[idx, "Найденные ПДн"] = "Нет никаких нарушений"
            continue

        info = row["Содержание"].lower()

        # Тут Даня пиши свой код выявления ПДн. 
    
    return df

def evaluate_violations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Функция принимает на вход датафрейм и по колонке найденные ПДн 
    оценивает опасность файлов. Оценка будет производиться согласно словарю
    с системой штрафов
    """
    
    pass

def run_scanning(path: str)->pd.DataFrame:

    """
    Функция для запуска полного сканнирования хранилища.
    Все функции расставлены в порядке их необходимого применения.
    В целом функции независимы, но это рекомендованный порядок

    """
    start_time = time.time()
    root_dir = path

    # --- Шаг 1 ---
    df = forming_table(root_dir=root_dir)
    time_step1 = time.time()
    print(f"Время извлечения метаданных: {round(time_step1 - start_time, 2)} сек.")

    # --- Шаг 2 ---
    extracted_df = parsing(df) 
    time_step2 = time.time()
    print(f"Время парсинга информации: {round(time_step2 - time_step1, 2)} сек.")

    # --- Шаг 3 ---
    found_danger_df = seek_danger(extracted_df) 
    time_step3 = time.time()
    print(f"Время анализа по №152-ФЗ: {round(time_step3 - time_step2, 2)} сек.")

    # Итог
    evaluated_df = evaluate_violations(found_danger_df)
    time_step4 = time.time() - start_time
    print(f"\nВремя оценки нарушений: {round(time_step4, 2)} сек.")

    print(extracted_df)

    try:
        conn = sqlite3.connect("DataBase.db")
        found_danger_df.to_sql("database", con = conn, if_exists = "replace")

    except Exception as e:

        print(f"Создать базу данных не удалось: {e}")

    finally:

        conn.close()

    # Итог
    total_time = time.time() - start_time
    print(f"\nОБЩЕЕ ВРЕМЯ РАБОТЫ: {round(total_time, 2)} сек.")



        