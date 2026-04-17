import os
import pandas as pd
import sqlite3

import fitz
import docx
from docx import Document
import whisper # Для аудио формата
import time
import datetime

import io
import tempfile
import subprocess
import pytesseract
from PIL import Image
from concurrent.futures import ThreadPoolExecutor


FFMPEG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ffmpeg.exe")
FFPROBE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ffprobe.exe")
pytesseract.pytesseract.tesseract_cmd = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "tesseract", "tesseract.exe"
)



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
            ".flac": "whisper", ".ogg": "whisper",

            # Группа 5: Изображения (через Tesseract OCR)
            ".png": "image_ocr", ".jpg": "image_ocr", ".jpeg": "image_ocr",
            ".bmp": "image_ocr", ".tiff": "image_ocr", ".tif": "image_ocr",
            ".webp": "image_ocr",

            # Группа 6: Видео (аудио Whisper + кадры Tesseract OCR)
            ".mp4": "video_engine", ".avi": "video_engine",
            ".mkv": "video_engine", ".mov": "video_engine",
            ".webm": "video_engine", ".wmv": "video_engine",

            # Группа 7: Таблицы (pandas - csv + openpyxl - xlsx + xlrd - xls)
            ".csv": "table_engine", ".tsv": "table_engine",
            ".xlsx": "table_engine", ".xls": "table_engine",

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

                    # OCR fallback для сканов
                    if len(text.strip()) / max(len(doc), 1) < 50:

                        def ocr_page(page):
                            pix = page.get_pixmap(matrix=fitz.Matrix(150/72, 150/72))
                            img = Image.open(io.BytesIO(pix.tobytes("png")))
                            return pytesseract.image_to_string(img, lang="rus+eng")

                        with ThreadPoolExecutor(max_workers=4) as pool:
                            results = list(pool.map(ocr_page, doc))
                        text = "\n\n".join(r.strip() for r in results if r.strip())

                    df.at[idx, "Содержание"] = text.strip() if text.strip() else "ПУСТОЙ ПДФ"
            except Exception as e:
                df.at[idx, "Содержание"] = f"Ошибка PDF: {e}"

        
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
        
        # Изображения OCR
        elif engine == "image_ocr":
            try:
                img = Image.open(path)
                if img.mode not in ("L", "RGB"):
                    img = img.convert("RGB")
                text = pytesseract.image_to_string(img, lang="rus+eng")
                df.at[idx, "Содержание"] = text.strip() if text.strip() else "OCR НЕ ИЗВЛЁК ТЕКСТ"
            except Exception as e:
                df.at[idx, "Содержание"] = f"Ошибка OCR: {e}"

        # Видео OCR+Whisper
        elif engine == "video_engine":
            try:
                results = []

                # аудиодорожка whisper
                with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
                    tmp_audio = tmp.name
                subprocess.run([
                    FFMPEG_PATH, "-i", path,
                    "-vn", "-acodec", "pcm_s16le",
                    "-ar", "16000", "-ac", "1", "-y", tmp_audio
                ], capture_output=True, timeout=120)

                if os.path.getsize(tmp_audio) > 1000:
                    audio = whisper.load_audio(tmp_audio)
                    mel = whisper.log_mel_spectrogram(
                        whisper.pad_or_trim(audio)
                    ).to(audio_model.device)
                    _, probs = audio_model.detect_language(mel)
                    lang = max(probs, key=probs.get)
                    res = audio_model.transcribe(tmp_audio, language=lang)
                    if res["text"].strip():
                        results.append(res["text"].strip())
                os.unlink(tmp_audio)

                # кадры OCR
                with tempfile.TemporaryDirectory() as tmpdir:
                    subprocess.run([
                        FFMPEG_PATH, "-i", path,
                        "-vf", "fps=1/10", "-q:v", "2", "-y",
                        os.path.join(tmpdir, "f_%04d.jpg")
                    ], capture_output=True, timeout=180)

                    prev = ""
                    for f in sorted(os.listdir(tmpdir)):
                        if not f.endswith(".jpg"):
                            continue
                        img = Image.open(os.path.join(tmpdir, f))
                        t = pytesseract.image_to_string(img, lang="rus+eng")
                        if t.strip() and t.strip() != prev:
                            results.append(t.strip())
                            prev = t.strip()

                df.at[idx, "Содержание"] = "\n".join(results) if results else "ВИДЕО: ТЕКСТ НЕ ИЗВЛЕЧЁН"
            except Exception as e:
                df.at[idx, "Содержание"] = f"Ошибка видео: {e}"

        # Таблицы
        elif engine == "table_engine":
            try:
                tbl_ext = os.path.splitext(path)[1].lower()

                if tbl_ext == ".csv":
                    for sep in [",", ";", "\t", "|"]:
                        try:
                            tdf = pd.read_csv(path, sep=sep, dtype=str, on_bad_lines="skip")
                            if len(tdf.columns) > 1:
                                break
                        except Exception:
                            continue
                    else:
                        tdf = pd.read_csv(path, dtype=str, on_bad_lines="skip")

                elif tbl_ext == ".tsv":
                    tdf = pd.read_csv(path, sep="\t", dtype=str, on_bad_lines="skip")

                elif tbl_ext in (".xlsx", ".xls"):
                    eng = "openpyxl" if tbl_ext == ".xlsx" else "xlrd"
                    sheets = pd.read_excel(path, sheet_name=None, dtype=str, engine=eng)
                    parts = []
                    for sname, sdf in sheets.items():
                        lines = [" ".join(str(c) for c in sdf.columns)]
                        for _, r in sdf.iterrows():
                            row_t = " ".join(str(v) for v in r.values if pd.notna(v))
                            if row_t.strip():
                                lines.append(row_t)
                        parts.append(f"[{sname}]\n" + "\n".join(lines))
                    df.at[idx, "Содержание"] = "\n\n".join(parts) if parts else "ПУСТАЯ ТАБЛИЦА"
                    continue

                lines = [" ".join(str(c) for c in tdf.columns)]
                for _, r in tdf.iterrows():
                    row_t = " ".join(str(v) for v in r.values if pd.notna(v))
                    if row_t.strip():
                        lines.append(row_t)
                df.at[idx, "Содержание"] = "\n".join(lines) if lines else "ПУСТАЯ ТАБЛИЦА"

            except Exception as e:
                df.at[idx, "Содержание"] = f"Ошибка таблицы: {e}"

    
    return df

def seek_danger(df: pd.DataFrame) -> pd.DataFrame:
    """
    функция seek_danger принимает на вход датафрейм с извлеченным текстом
    и возвращает датафрейм с измененной колонкой "найденные пдн". в эту колонку
    записываем все, что совпало с ключами (согласно фз №152), через запятую.
    """
    
    import re
    
    # Словарь паттернов для поиска различных типов персональных данных (ПДн)
    patterns = {
        # Паспорт: 2 цифры, пробел (опционально), 2 цифры, пробел (опционально), 6 цифр
        "Паспорт": r"\d{2}\s?\d{2}\s?\d{6}",
        
        # СНИЛС: 3 цифры, дефис, 3 цифры, дефис, 3 цифры, пробел, 2 цифры
        "СНИЛС": r"\d{3}-\d{3}-\d{3}\s\d{2}",
        
        # Телефон: российские номера
        "Телефон": r"(?:\+7|8)[\s\(-]*\d{3}[\s\)-]*\d{3}[\s-]*\d{2}[\s-]*\d{2}",
        
        # ИНН: 10 или 12 цифр
        "ИНН": r"\b\d{10}\b|\b\d{12}\b",
        
        # Email
        "Email": r"[\w\.-]+@[\w\.-]+\.\w+",
        
        # Медицина
        "Медицина: Диагноз/Анамнез": r"(?i)\b(?:диагноз|анамнез|жалобы|лечение|терапия|мкб-\d+)\b",
        "Медицина: Полис ОМС": r"\b\d{16}\b",
        "Медицина: Рецепт/Препараты": r"(?i)\b(?:рецепт|назначено|мг/сут|таблетки|дозировка)\b",
        "Медицина: Медучреждение": r"(?i)\b(?:больница|поликлиника|медцентр|клиника|врач-[\w]+)\b",
        
        # Дополнительные типы ПДн
        "ФИО": r"(?:[А-Я][а-я]+\s+[А-Я][а-я]+\s+[А-Я][а-я]+)|(?:[А-Я][а-я]+\s+[А-Я]\.\s*[А-Я]\.)",
        "Дата рождения": r"\b(?:0[1-9]|[12][0-9]|3[01])[./-](?:0[1-9]|1[0-2])[./-](?:19|20)\d{2}\b",
        "Адрес регистрации": r"(?i)(?:адрес|зарегистрирован|проживает|прописка)\s*:?\s*"
                             r"(?:г\.|города|ул\.|улица|д\.|дом|кв\.|квартира)",
        "Заработная плата": r"(?i)(?:зарплата|оклад|доход|зп)\s*:?\s*\d+",
        "Состояние здоровья": r"(?i)\b(?:диагноз|заболевание|болезнь)\s*:?\s*[А-Яа-я\s,]+",
    }
    
    for idx, row in df.iterrows():
        # Проверка на ошибку
        if str(row["Содержание"]).split(" ")[0] == "Ошибка":
            df.at[idx, "Найденные ПДн"] = "Нет никаких нарушений"
            continue
        
        # НЕ ПРИВОДИМ К НИЖНЕМУ РЕГИСТРУ для поиска! 
        # Используем оригинальный текст для паттернов, где важны цифры
        info = str(row["Содержание"])
        info_lower = info.lower()  # Отдельно для регистронезависимых паттернов
        
        found_pdns = []
        
        for pattern_name, pattern in patterns.items():
            try:
                # Для паттернов, где важен регистр (ФИО, адреса) используем info_lower
                if pattern_name in ["ФИО", "Адрес регистрации", "Заработная плата", 
                                    "Состояние здоровья", "Медицина: Диагноз/Анамнез",
                                    "Медицина: Рецепт/Препараты", "Медицина: Медучреждение"]:
                    text_to_search = info_lower
                else:
                    # Для цифровых паттернов (паспорт, СНИЛС, ИНН, телефон, email) используем оригинал
                    text_to_search = info
                
                if re.search(pattern, text_to_search, re.IGNORECASE):
                    found_pdns.append(pattern_name)
            except Exception as e:
                # Если ошибка, пропускаем
                continue
        
        # Запись результата
        if found_pdns:
            # Убираем дубликаты
            unique_pdns = []
            for pd_type in found_pdns:
                if pd_type not in unique_pdns:
                    unique_pdns.append(pd_type)
            df.at[idx, "Найденные ПДн"] = ",".join(unique_pdns)
        else:
            df.at[idx, "Найденные ПДн"] = "Нет никаких нарушений"
    
    return df


def evaluate_violations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Функция принимает на вход датафрейм и по колонке найденные ПДн 
    оценивает опасность файлов в баллах от 0 до 10.
    
    Шкала опасности:
    0      - нет нарушений
    1-2    - низкая опасность (отдельные ФИО, телефон, email)
    3-4    - ниже среднего (ФИО+email, дата рождения+ФИО)
    5-6    - средняя опасность (ФИО+телефон, ФИО+адрес)
    7-8    - высокая опасность (паспорт один, зарплата+ФИО, медицина+ФИО)
    9-10   - критическая опасность (паспорт+телефон, ФИО+СНИЛС, медицина+паспорт)
    """
    
    from itertools import combinations
    
    # ========================================================================
    # 1. БАЗОВЫЕ БАЛЛЫ ДЛЯ ОДИНОЧНЫХ ТИПОВ ПДн
    # ========================================================================
    BASE_SCORES = {
        # Низкая опасность (1-2 балла)
        "ФИО": 1,
        "Телефон": 1,
        "Email": 1,
        "Дата рождения": 1,
        "Адрес регистрации": 2,
        "Полис ОМС": 2,
        "Медицина: Медучреждение": 2,
        
        # Средняя опасность (3-5 баллов)
        "ИНН": 4,
        "Заработная плата": 5,
        
        # Высокая опасность (6-8 баллов)
        "Паспорт": 7,
        "СНИЛС": 7,
        "Состояние здоровья": 8,
        "Медицина: Диагноз/Анамнез": 8,
        "Медицина: Рецепт/Препараты": 7,
    }
    
    # ========================================================================
    # 2. БАЛЛЫ ДЛЯ КОМБИНАЦИЙ ДВУХ ТИПОВ ПДн
    # ========================================================================
    # ПРОСТОЕ РЕШЕНИЕ: прописываем оба порядка для каждой важной пары
    # ========================================================================
    COMBINATION_SCORES = {
        # ===== ФИО + что-то (критически важно!) =====
        ("ФИО", "СНИЛС"): 10,
        ("СНИЛС", "ФИО"): 10,        # Явно добавляем обратный порядок
        ("ФИО", "Паспорт"): 10,
        ("Паспорт", "ФИО"): 10,       # Явно добавляем обратный порядок
        
        # ===== Паспорт + СНИЛС =====
        ("Паспорт", "СНИЛС"): 10,
        ("СНИЛС", "Паспорт"): 10,     # Явно добавляем обратный порядок
        
        # ===== Медицина + документы =====
        ("Состояние здоровья", "Паспорт"): 10,
        ("Паспорт", "Состояние здоровья"): 10,
        ("Состояние здоровья", "СНИЛС"): 10,
        ("СНИЛС", "Состояние здоровья"): 10,
        ("Состояние здоровья", "ФИО"): 8,
        ("ФИО", "Состояние здоровья"): 8,
        
        # ===== Документы + телефон (высокая опасность) =====
        ("Паспорт", "Телефон"): 8,
        ("Телефон", "Паспорт"): 8,
        ("СНИЛС", "Телефон"): 8,
        ("Телефон", "СНИЛС"): 8,
        
        # ===== ФИО + другие данные (средняя опасность) =====
        ("ФИО", "Телефон"): 5,
        ("Телефон", "ФИО"): 5,
        ("ФИО", "Адрес регистрации"): 6,
        ("Адрес регистрации", "ФИО"): 6,
        ("ФИО", "ИНН"): 7,
        ("ИНН", "ФИО"): 7,
        ("ФИО", "Заработная плата"): 7,
        ("Заработная плата", "ФИО"): 7,
        ("ФИО", "Email"): 4,
        ("Email", "ФИО"): 4,
        ("ФИО", "Дата рождения"): 4,
        ("Дата рождения", "ФИО"): 4,
        ("ФИО", "Полис ОМС"): 5,
        ("Полис ОМС", "ФИО"): 5,
        
        # ===== Паспорт + другие документы (высокая опасность) =====
        ("Паспорт", "ИНН"): 9,
        ("ИНН", "Паспорт"): 9,
        ("СНИЛС", "ИНН"): 9,
        ("ИНН", "СНИЛС"): 9,
        ("Паспорт", "Заработная плата"): 9,
        ("Заработная плата", "Паспорт"): 9,
        ("СНИЛС", "Заработная плата"): 9,
        ("Заработная плата", "СНИЛС"): 9,
        
        # ===== Комбинации без ФИО (средняя опасность) =====
        ("Телефон", "Адрес регистрации"): 5,
        ("Адрес регистрации", "Телефон"): 5,
        ("Телефон", "Email"): 3,
        ("Email", "Телефон"): 3,
    }
    
    # Бонус за 3+ типа
    THREE_PLUS_BONUS = 1.5
    MAX_SCORE = 10.0
    
    # ========================================================================
    # 3. ОСНОВНОЙ ЦИКЛ ОБРАБОТКИ
    # ========================================================================
    for idx, row in df.iterrows():
        found_pdns_str = row["Найденные ПДн"]
        
        # Если нарушений нет - рейтинг 0
        if found_pdns_str == "Нет никаких нарушений":
            df.at[idx, "Рейтинг опасности"] = 0.0
            continue
        
        # Разбиваем строку по запятым и убираем лишние пробелы
        found_pdns_list = [t.strip() for t in found_pdns_str.split(",")]
        
        # ====================================================================
        # ШАГ 1: Находим максимальный балл среди одиночных типов
        # ====================================================================
        max_single_score = max(BASE_SCORES.get(t, 1) for t in found_pdns_list)
        total_score = max_single_score
        
        # ====================================================================
        # ШАГ 2: Проверяем все возможные пары (прямой поиск без нормализации)
        # ====================================================================
        best_pair_score = 0
        for pd1, pd2 in combinations(found_pdns_list, 2):
            # Ищем пару в обоих порядках (они явно прописаны в словаре)
            if (pd1, pd2) in COMBINATION_SCORES:
                pair_score = COMBINATION_SCORES[(pd1, pd2)]
                if pair_score > best_pair_score:
                    best_pair_score = pair_score
        
        # Берём максимальный балл
        if best_pair_score > total_score:
            total_score = best_pair_score
        
        # ====================================================================
        # ШАГ 3: Бонус за 3+ типов
        # ====================================================================
        if len(found_pdns_list) >= 3 and total_score < 8:
            total_score = min(total_score + THREE_PLUS_BONUS, MAX_SCORE)
        
        # ====================================================================
        # ШАГ 4: Финальная запись
        # ====================================================================
        df.at[idx, "Рейтинг опасности"] = round(min(total_score, MAX_SCORE), 1)
    
    return df
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
    print("Готово!")
    print(evaluated_df)

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
    
if __name__ == "__main__":
    path_to_scan = r"C:\Hacaton\dataTest"
    run_scanning(path_to_scan)
    print("Готово!")