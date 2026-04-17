import os
import pandas as pd
import sqlite3

import fitz
import docx
from docx import Document
import whisper # Для аудио формата
import time
import datetime


import cv2
import numpy as np
import mediapipe as mp
import io
import tempfile
import subprocess
import pytesseract
from PIL import Image
from concurrent.futures import ThreadPoolExecutor
import re

FFMPEG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ffmpeg.exe")
FFPROBE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ffprobe.exe")
pytesseract.pytesseract.tesseract_cmd = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "tesseract", "tesseract.exe"
)

mp_face_detection = mp.solutions.face_detection
mp_pose = mp.solutions.pose

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


def parsing(df: pd.DataFrame) -> pd.DataFrame:
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

            # Группа 5: Изображения (через Tesseract OCR + MediaPipe биометрия)
            ".png": "image_ocr", ".jpg": "image_ocr", ".jpeg": "image_ocr",
            ".bmp": "image_ocr", ".tiff": "image_ocr", ".tif": "image_ocr",
            ".webp": "image_ocr",

            # Группа 6: Видео (аудио Whisper + кадры Tesseract OCR + MediaPipe биометрия)
            ".mp4": "video_engine", ".avi": "video_engine",
            ".mkv": "video_engine", ".mov": "video_engine",
            ".webm": "video_engine", ".wmv": "video_engine",

            # Группа 7: Таблицы (pandas - csv + openpyxl - xlsx + xlrd - xls)
            ".csv": "table_engine", ".tsv": "table_engine",
            ".xlsx": "table_engine", ".xls": "table_engine",

            # Группа 8: Бинарники (извлечение читаемых строк)
            ".exe": "binary_engine", ".dll": "binary_engine",
            ".bin": "binary_engine", ".dat": "binary_engine",
        }
        return cases.get(extension, "skip")

    # Вспомогательные функции детекции биометрии

    def _detect_signature(gray) -> bool:
        """
        Эвристика: ищем рукописную подпись в нижней трети изображения.
        Анализируем контуры — подпись это вытянутая кривая линия.
        """
        h, w = gray.shape
        bottom = gray[int(h * 0.65):, :]
        if bottom.size == 0:
            return False
        _, binary = cv2.threshold(bottom, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
        contours, _ = cv2.findContours(binary, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        for c in contours:
            x, y, cw, ch = cv2.boundingRect(c)
            arc = cv2.arcLength(c, False)
            area = cv2.contourArea(c)
            if (cw > ch * 1.5 and 30 < cw < w * 0.6
                    and 5 < ch < h * 0.15 and arc > 50
                    and area < cw * ch * 0.5):
                return True
        return False

    def _detect_fingerprint(gray) -> bool:
        """
        Эвристика: детекция отпечатка пальца через Gabor-фильтры.
        Отпечаток — это полосатая текстура с сильным откликом по многим направлениям.
        """
        small = cv2.resize(gray, (300, 300))
        responses = []
        for theta in np.arange(0, np.pi, np.pi / 8):
            kernel = cv2.getGaborKernel((21, 21), sigma=4.0, theta=theta, lambd=8.0, gamma=0.5, psi=0)
            filtered = cv2.filter2D(small, cv2.CV_8UC3, kernel)
            responses.append(filtered.mean())
        return sum(1 for r in responses if r > 30) >= 5

    def detect_biometry(path: str) -> list:
        """
        Комбинированная детекция биометрии в изображении:
        - MediaPipe: лицо, глаза, силуэт тела (нейросеть)
        - OpenCV: подпись и отпечаток пальца (эвристики)
        Возвращает список найденных типов, например: ["лицо (2)", "глаза", "подпись"]
        """
        img = cv2.imread(path)
        if img is None:
            return []

        found = []
        rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

        # Лицо + глаза (MediaPipe Face Detection)
        try:
            with mp_face_detection.FaceDetection(model_selection=1, min_detection_confidence=0.5) as detector:
                results = detector.process(rgb)
                if results.detections:
                    found.append(f"лицо ({len(results.detections)})")
                    for det in results.detections:
                        kp = det.location_data.relative_keypoints
                        if len(kp) >= 2:  # правый глаз + левый глаз
                            found.append("глаза")
                            break
        except Exception:
            pass

        # Силуэт тела (MediaPipe Pose)
        try:
            with mp_pose.Pose(static_image_mode=True, min_detection_confidence=0.5) as pose:
                if pose.process(rgb).pose_landmarks:
                    found.append("силуэт тела")
        except Exception:
            pass

        # Подпись (OpenCV эвристика контуров)
        try:
            if _detect_signature(gray):
                found.append("подпись")
        except Exception:
            pass

        # Отпечаток пальца (OpenCV Gabor-фильтры)
        try:
            if _detect_fingerprint(gray):
                found.append("отпечаток пальца")
        except Exception:
            pass

        return found

    def extract_binary(path: str, min_length: int = 6) -> str:
        """
        Извлекает читаемые ASCII-строки из бинарного файла.
        Нужно для поиска ПДн внутри .exe, .dll и других бинарников.
        """
        try:
            with open(path, "rb") as f:
                raw = f.read()
            ascii_strings = re.findall(rb'[ -~]{%d,}' % min_length, raw)
            result = []
            for s in ascii_strings:
                try:
                    result.append(s.decode("ascii"))
                except Exception:
                    pass
            return "\n".join(result) if result else "БИНАРНИК: ЧИТАЕМЫХ СТРОК НЕ НАЙДЕНО"
        except Exception as e:
            return f"Ошибка бинарника: {e}"

    # Загрузка модели Whisper (один раз на весь парсинг)

    audio_model = whisper.load_model("base")

    # Основной цикл по файлам

    for idx, row in df.iterrows():

        path = row["Путь"]
        path = os.path.normpath(path=path)
        ext = row["Расширение"]
        engine = choose_engine(ext)

        if not is_file_accessible(path) or os.path.getsize(path) == 0:

            df.at[idx, "Содержание"] = "ПУСТОЙ ФАЙЛ" if os.path.getsize(path) == 0 else "НЕТ ДОСТУПА"
            continue

        # PDF (PyMuPDF + OCR fallback для сканов)

        if engine == "pdf_engine":
            try:
                with fitz.open(path) as doc:
                    text = ""
                    for page in doc:
                        text += page.get_text()

                    # OCR fallback для сканов
                    # если среднее кол-во символов на страницу < 50, значит это скан
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

        #Word (python-docx)
        
        elif engine == "docx_engine":
            try:
                doc = Document(path)
                text = "\n".join([p.text for p in doc.paragraphs])
                df.at[idx, "Содержание"] = text.strip() if text else "ПУСТОЙ DOCX"
            except Exception as e:
                print(f"Ошибка в чтении файла DOCx: {e}")
                df.at[idx, "Содержание"] = f"Ошибка в чтении файла DOCx: {e}"
        
        # Текст (встроенный open)

        elif engine == "text_engine":
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as txt:
                    df.at[idx, "Содержание"] = txt.read().strip()
            except Exception as e:
                print(f"Ошибка в чтении файла текстового формата: {e}")
                df.at[idx, "Содержание"] = f"Ошибка в чтении файла текстового формата: {e}"

        # Аудио (Whisper + пометка биометрии голоса)

        elif engine == "whisper":

            try:
                audio = whisper.load_audio(path)
                audio_segment = whisper.pad_or_trim(audio)

                mel = whisper.log_mel_spectrogram(audio_segment).to(audio_model.device)
                _, probs = audio_model.detect_language(mel)

                detected_language = max(probs, key=probs.get)

                result = audio_model.transcribe(path, language=detected_language)

                text = result["text"].strip() if result else ""
                if text:
                    # Голос — это биометрические ПДн согласно ФЗ №152 ст.11
                    text += "\n[БИОМЕТРИЯ: образец голоса]"
                    df.at[idx, "Содержание"] = text
                else:
                    df.at[idx, "Содержание"] = "НИЧЕГО НЕ ИЗВЛЕЧЕНО"
            
            except Exception as e:

                print(f"Произошел сбой при извлечении аудиодорожки: {e}")
                df.at[idx, "Содержание"] = f"Произошел сбой при извлечении аудиодорожки: {e}"
        
        # Изображения (Tesseract OCR + MediaPipe/OpenCV биометрия)

        elif engine == "image_ocr":
            try:
                img = Image.open(path)
                if img.mode not in ("L", "RGB"):
                    img = img.convert("RGB")
                text = pytesseract.image_to_string(img, lang="rus+eng")

                # Детекция биометрии: лицо, глаза, силуэт, подпись, отпечаток
                bio = detect_biometry(path)
                if bio:
                    text += f"\n[БИОМЕТРИЯ: {', '.join(bio)}]"

                df.at[idx, "Содержание"] = text.strip() if text.strip() else "OCR НЕ ИЗВЛЁК ТЕКСТ"
            except Exception as e:
                df.at[idx, "Содержание"] = f"Ошибка OCR: {e}"

        # Видео (ffmpeg аудио - Whisper - кадры - OCR + биометрия)

        elif engine == "video_engine":
            try:
                results = []

                # Извлекаем аудиодорожку из видео через ffmpeg - транскрибируем Whisper
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
                        # Голос из видео — тоже биометрия
                        results.append("[БИОМЕТРИЯ: образец голоса]")
                os.unlink(tmp_audio)

                # Извлекаем кадры каждые 10 секунд - OCR - детекция биометрии
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

                        frame_path = os.path.join(tmpdir, f)
                        img = Image.open(frame_path)
                        t = pytesseract.image_to_string(img, lang="rus+eng")
                        if t.strip() and t.strip() != prev:
                            results.append(t.strip())
                            prev = t.strip()

                        # Проверяем каждый кадр на биометрию
                        bio = detect_biometry(frame_path)
                        if bio:
                            results.append(f"[БИОМЕТРИЯ кадр {f}: {', '.join(bio)}]")

                df.at[idx, "Содержание"] = "\n".join(results) if results else "ВИДЕО: ТЕКСТ НЕ ИЗВЛЕЧЁН"
            except Exception as e:
                df.at[idx, "Содержание"] = f"Ошибка видео: {e}"

        # Таблицы (pandas csv/tsv + openpyxl xlsx + xlrd xls)

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

        # Бинарники (извлечение читаемых строк)

        elif engine == "binary_engine":
            try:
                df.at[idx, "Содержание"] = extract_binary(path)
            except Exception as e:
                df.at[idx, "Содержание"] = f"Ошибка бинарника: {e}"

    return df


def seek_danger(df: pd.DataFrame) -> pd.DataFrame:
    """
    функция seek_danger принимает на вход датафрейм с извлеченным текстом
    и возвращает датафрейм с измененной колонкой "найденные пдн". в эту колонку
    записываем все, что совпало с ключами (согласно фз №152), через запятую.
    """
    
    # Словарь паттернов для поиска различных типов персональных данных (ПДн)
    # Ключ = название типа ПДн, значение = регулярное выражение (regex) для поиска
    patterns = {
        # Паспорт: 2 цифры, пробел (опционально), 2 цифры, пробел (опционально), 6 цифр
        # Пример: 12 34 567890 или 1234567890
        "Паспорт": r"\d{2}\s?\d{2}\s?\d{6}",
        
        # СНИЛС: 3 цифры, дефис, 3 цифры, дефис, 3 цифры, пробел, 2 цифры
        # Пример: 123-456-789 01
        "СНИЛС": r"\d{3}-\d{3}-\d{3}\s\d{2}",
        
        # Телефон: российские номера с +7 или 8, с возможными разделителями (пробелы, скобки, дефисы)
        # Примеры: +7-912-345-67-89, 89123456789, 8 (912) 345-67-89
        "Телефон": r"(?:\+7|8)[\s\(-]*\d{3}[\s\)-]*\d{3}[\s-]*\d{2}[\s-]*\d{2}",
        
        # ИНН: 10 или 12 цифр как отдельное слово (границы слова \b)
        # Пример: 1234567890 (ИНН юрлица) или 123456789012 (ИНН физлица)
        "ИНН": r"\b\d{10}\b|\b\d{12}\b",
        
        # Email: стандартный формат email (имя@домен.зона)
        # Пример: user@example.com, name.surname@mail.ru
        "Email": r"[\w\.-]+@[\w\.-]+\.\w+",
        
        # Медицинские диагнозы и анамнез: ключевые слова (регистронезависимо через (?i))
        "Медицина: Диагноз/Анамнез": r"(?i)\b(?:диагноз|анамнез|жалобы|лечение|терапия|мкб-\d+)\b",
        
        # Полис ОМС: 16 цифр подряд (единый номер полиса)
        "Медицина: Полис ОМС": r"\b\d{16}\b",
        
        # Рецепты и препараты: ключевые слова (регистронезависимо)
        "Медицина: Рецепт/Препараты": r"(?i)\b(?:рецепт|назначено|мг/сут|таблетки|дозировка)\b",
        
        # Медицинские учреждения: ключевые слова (регистронезависимо)
        "Медицина: Медучреждение": r"(?i)\b(?:больница|поликлиника|медцентр|клиника|врач-[\w]+)\b"
    }
    
    import re  # Импортируем модуль для работы с регулярными выражениями
    
    # Перебираем каждую строку (каждый файл/документ) в датафрейме
    for idx, row in df.iterrows():
        
        # ПРОВЕРКА НА ОШИБКУ: если текст начинается со слова "Ошибка"
        # Это служебная метка, такой текст не нужно анализировать
        if str(row["Содержание"]).split(" ")[0] == "Ошибка":
            df.at[idx, "Найденные ПДн"] = "Нет никаких нарушений"
            continue  # Переходим к следующей строке
        
        # Приводим текст к нижнему регистру для регистронезависимого поиска
        info = str(row["Содержание"]).lower()
        
        # Список для хранения найденных типов ПДн в текущем документе
        found_pdns = []
        
        # Перебираем все паттерны из словаря
        for pattern_name, pattern in patterns.items():
            try:
                # Ищем паттерн в тексте (re.IGNORECASE - игнорируем регистр)
                if re.search(pattern, info, re.IGNORECASE):
                    # Если нашли - добавляем название типа ПДн в список
                    found_pdns.append(pattern_name)
            except Exception:
                # Если произошла ошибка при поиске (например, битый паттерн) - пропускаем
                continue
        
        # ЗАПИСЬ РЕЗУЛЬТАТА:
        if found_pdns:
            # Если что-то нашли - объединяем список через запятую в строку
            # Пример: "Паспорт,Телефон,Email"
            df.at[idx, "Найденные ПДн"] = ",".join(found_pdns)
        else:
            # Если ничего не нашли - записываем фразу об отсутствии нарушений
            df.at[idx, "Найденные ПДн"] = "Нет никаких нарушений"
    
    # Возвращаем изменённый датафрейм (с новой колонкой "Найденные ПДн")
    return df


def evaluate_violations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Функция принимает на вход датафрейм и по колонке найденные ПДн 
    оценивает опасность файлов. Оценка будет производиться согласно словарю
    с системой штрафов. Функция возвращает исходную таблицу с данными о сумме штрафов.
    """
    
    # Словарь штрафов за каждый тип ПДн (в рублях)
    # Штрафы условные, основаны на серьёзности нарушения
    FINES = {
        "Паспорт": 5000,                    # Документ, удостоверяющий личность
        "СНИЛС": 4000,                      # Номер пенсионного страхования
        "Телефон": 1000,                    # Контактный номер (наименьший штраф)
        "ИНН": 3000,                        # Идентификационный номер налогоплательщика
        "Email": 800,                       # Адрес электронной почты (самый низкий штраф)
        "Медицина: Диагноз/Анамнез": 7000,  # Медицинские диагнозы (чувствительные данные)
        "Медицина: Полис ОМС": 2000,        # Номер полиса медицинского страхования
        "Медицина: Рецепт/Препараты": 6000, # Информация о лечении и лекарствах
        "Медицина: Медучреждение": 5000,    # Названия медицинских учреждений
    }
    
    # Перебираем каждую строку датафрейма
    for idx, row in df.iterrows():
        
        # Получаем строку с найденными ПДн из предыдущей функции
        found_pdns_str = row["Найденные ПДн"]
        
        # Если нарушений нет - рейтинг опасности = 0
        if found_pdns_str == "Нет никаких нарушений":
            df.at[idx, "Рейтинг опасности"] = 0.0
            continue  # Переходим к следующей строке
        
        # Разбиваем строку по запятым (т.к. в seek_danger мы объединяли через ",")
        # Пример: "Паспорт,Телефон" -> ["Паспорт", "Телефон"]
        found_pdns_list = found_pdns_str.split(",")
        
        # Суммируем штрафы за все найденные типы ПДн
        total_fine = 0
        
        # Для каждого типа ПДн из списка
        for pd_type in found_pdns_list:
            # Добавляем соответствующий штраф из словаря FINES
            # Если типа нет в словаре - возникнет KeyError (это хорошо, сразу увидим проблему)
            total_fine += FINES[pd_type]
        
        # Записываем итоговую сумму штрафа в колонку "Рейтинг опасности"
        # Преобразуем в float для единообразия (хотя сумма целая)
        df.at[idx, "Рейтинг опасности"] = float(total_fine)
    
    # Возвращаем датафрейм с добавленной колонкой "Рейтинг опасности"
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