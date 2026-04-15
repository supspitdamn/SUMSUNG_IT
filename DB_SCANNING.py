import os
import pandas as pd
import sqlite3

import fitz
import docx
from docx import Document
import whisper # Для аудио формата
import re
import time
import datetime

audio_model = whisper.load_model("base")

def is_file_accessible(path: str) -> bool:
    """
    Проверяет реальную возможность чтения файла.
    os.access часто врет в Windows, поэтому используем попытку открытия.
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
    ФУНКЦИЯ forming_table ПРИНИМАЕТ НА ВХОД ССЫЛКУ НА КОРНЕВУЮ ПАПКУ (УСЛОВНАЯ БАЗА ДАННЫХ) И ВОЗВРАЩАЕТ
    ТАБЛИЦУ С МЕТАДАННЫМИ О КАЖДОМ ФАЙЛЕ (НАЗВАНИЕ, РАСШИРЕНИЕ, ПУТЬ). ТАКЖЕ СОЗДАЕТ ДОПОЛНИТЕЛЬНО КОЛОНКИ
    ОЦЕНКИ ОПАСНОТИ, СОДЕРЖАНИЯ, КАТЕГОРИИ
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
                df['Категория'] = "Unkwown"
                df['Рейтинг опасности'] = 0.0
                df['Найденные ПДн'] = "NO"
    return df

def parsing(df: pd.DataFrame) -> None:

    """
    ФУНКЦИЯ PARSING ПРИНИМАЕТ НА ВХОД ТАБЛИЦУ В PANDAS И ВОЗВРАЩАЕТ ИЗМЕНЕННЫЙ ДАТАФРЕЙМ
    В ФУНКЦИИ ПАРСИНГ ЕСТЬ ВЛОЖЕННАЯ СЛУЖЕБНАЯ ФУНКЦИЯ CHOOSE_ENGINE, ПРИНИМАЮЩАЯ
    НА ВХОД СТРОКУ (РАСШИРЕНИЕ ФАЙЛА) И ВОЗВРАЩАЕТ В ВИДЕ СТРОКИ ТИП ИСПОЛЬЗУЕМОГО ПАРСЕРА
    ЦИКЛ ИТЕРАТИВНО ПЕРЕБИРАЕТ СТРОКИ ТАБЛИЦЫ БЕРЕТ РАСШИРЕНИЕ И ССЫЛКУ НА ФАЙЛ, ОПРЕДЕЛЯЕТ 
    ТИП ПАРСЕРА И ИЗВЛЕКАЕТ ТЕКСТ В СООТВЕТСТВУЮЩУЮ ЯЧЕЙКУ СОДЕРЖАНИЕ
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

def seek_danger(df: pd.Dataframe) -> pd.DataFrame:
    """
    ФУНКЦИЯ seek_danger ПРИНИМАЕТ НА ВХОД ДАТАФРЕЙМ С ИЗВЛЕЧЕННЫМ ТЕКСТОМ
    И ВОЗВРАЩАЕТ ДАТАФРЕЙМ С ИЗМЕНЕННОЙ КОЛОНКОЙ "Найденные ПДн". В ЭТУ КОЛОНКУ
    ЗАПИСЫВАЕМ ВСЕ, ЧТО СОВПАЛО С КЛЮЧАМИ (СОГЛАСНО ФЗ.152).
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

    

root_dir = ".//DataBase"

df = forming_table(root_dir=root_dir) # Извлечение метаданных

extracted_df = parsing(df) # Получаем датафрей с извлеченными файлами

found_dangers_df = seek_danger(df) # Получаем датафрейм с колонкой нарушений

print(extracted_df["Содержание"].iloc[4])

print(extracted_df[["Имя файла", "Содержание"]])







        