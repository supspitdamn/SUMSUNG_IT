import os
import pandas as pd
import sqlite3

import fitz
import docx
from docx import Document
import whisper # Для аудио формата
import time
import datetime
import xml.etree.ElementTree as ET
import io
import tempfile
import subprocess
import pytesseract
import polars
from PIL import Image
from concurrent.futures import ThreadPoolExecutor
import cv2
import numpy as np
import re
import mediapipe as mp

from striprtf.striprtf import rtf_to_text

import concurrent.futures
import json

import concurrent.futures
import json

FFMPEG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ffmpeg.exe")
FFPROBE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ffprobe.exe")
pytesseract.pytesseract.tesseract_cmd = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "tesseract", "tesseract.exe"
)

VALID_BICS = set()
BIC_TO_BANK_INFO = {}

# Глобальные переменные для хранения инициализированных объектов (вне функции)
_NATASHA_INITIALIZED = False
_NATASHA_SEGMENTER = None
_NATASHA_MORPH_VOCAB = None
_NATASHA_NER_TAGGER = None
_NATASHA_DATES_EXTRACTOR = None
_VALID_BICS = set()

def _init_natasha():
    """Инициализация Natasha (вызывается один раз)"""
    global _NATASHA_INITIALIZED, _NATASHA_SEGMENTER, _NATASHA_MORPH_VOCAB
    global _NATASHA_NER_TAGGER, _NATASHA_DATES_EXTRACTOR
    
    if _NATASHA_INITIALIZED:
        return
    
    try:
        from natasha import (
            Segmenter, MorphVocab, NewsEmbedding,
            NewsNERTagger, DatesExtractor, Doc
        )
        
        print("Загрузка моделей Natasha (может занять 10-15 секунд)...")
        _NATASHA_SEGMENTER = Segmenter()
        _NATASHA_MORPH_VOCAB = MorphVocab()
        emb = NewsEmbedding()
        _NATASHA_NER_TAGGER = NewsNERTagger(emb)
        _NATASHA_DATES_EXTRACTOR = DatesExtractor(_NATASHA_MORPH_VOCAB)
        
        _NATASHA_INITIALIZED = True
        print("Модели Natasha загружены успешно")
    except ImportError:
        print("Предупреждение: библиотека natasha не установлена. NLP-функции будут отключены.")
        _NATASHA_INITIALIZED = True  # Отмечаем как инициализированное, но с ошибкой
    except Exception as e:
        print(f"Ошибка инициализации Natasha: {e}")
        _NATASHA_INITIALIZED = True


def _init_bic_directory():
    """Инициализация справочника БИК (вызывается один раз)"""
    global _VALID_BICS
    
    if _VALID_BICS:
        return
    
    import xml.etree.ElementTree as ET
    
    try:
        tree = ET.parse("20260417_ED807_full.xml")
        root = tree.getroot()
        ns = {'ed': 'urn:cbr-ru:ed:v2.0'}
        for bic_entry in root.findall('.//ed:BICDirectoryEntry', ns):
            bic = bic_entry.get('BIC')
            if bic:
                _VALID_BICS.add(bic)
        print(f"Загружено {len(_VALID_BICS)} БИК из справочника")
    except Exception as e:
        print(f"Ошибка загрузки справочника БИК: {e}")
        
mp_face_detection = mp.solutions.face_detection
mp_pose = mp.solutions.pose

# ====================================================================
# МОДЕЛИ ДЛЯ ДЕТЕКЦИИ БИОМЕТРИИ (загружаются один раз при старте)
# ====================================================================
from huggingface_hub import hf_hub_download
from ultralytics import YOLO
from transformers import pipeline as hf_pipeline

# YOLOv8s — детекция рукописных подписей (~22 МБ, mAP@50 = 94.5%)
try:
    _sig_model_path = hf_hub_download(
        repo_id="tech4humans/yolov8s-signature-detector",
        filename="yolov8s.pt"
    )
    _sig_model = YOLO(_sig_model_path)
    print("Модель подписей: OK")
except Exception as e:
    _sig_model = None
    print(f"Модель подписей не загружена: {e}")

# SigLIP — zero-shot классификация отпечатков пальцев (~400 МБ)
try:
    _fp_classifier = hf_pipeline(
        "zero-shot-image-classification",
        model="google/siglip-base-patch16-224"
    )
    _fp_labels = ["a fingerprint", "a document with text", "a photo of a person"]
    print("Модель отпечатков: OK")
except Exception as e:
    _fp_classifier = None
    print(f"Модель отпечатков не загружена: {e}")


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
        df['Требуемый УЗ'] = 0.0
        df['Найденные ПДн'] = "NO"
        df['Категории'] = "NO"

    return df

### Служебные функции
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
            ".gif": "image_ocr",

            # JSON (встроенный json)
            ".json": "json_engine",

            # RTF (через striprtf)
            ".rtf": "rtf_engine",

            # DOC старый формат
            ".doc": "doc_engine",

            ".xls": "table_engine",

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

            ".parquet" : "binary_engine"
        }
        return cases.get(extension, "skip")

def flatten_json(obj, prefix=""):
                    """Рекурсивно разворачиваем JSON в плоский текст"""
                    parts = []
                    if isinstance(obj, dict):
                        for k, v in obj.items():
                            parts.extend(flatten_json(v, f"{prefix}{k}: "))
                    elif isinstance(obj, list):
                        for item in obj:
                            parts.extend(flatten_json(item, prefix))
                    else:
                        parts.append(f"{prefix}{obj}")
                    return parts

def extract_binary(path: str, min_length: int = 6) -> str:
    """
    Извлекает читаемые ASCII-строки из бинарного файла.
    Нужно для поиска ПДн внутри .doc и других бинарников.
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

def _detect_signature(path: str) -> bool:
        """
        Детекция подписи через YOLOv8s (обучена на 10k+ образцов подписей).
        Возвращает True если найдена хотя бы одна подпись с confidence > 0.5
        """
        if _sig_model is None:
            return False
        try:
            results = _sig_model(path, verbose=False)
            for r in results:
                for box in r.boxes:
                    if float(box.conf[0]) > 0.5:
                        return True
        except Exception:
            pass
        return False

def _detect_fingerprint(path: str) -> bool:
        """
        Детекция отпечатка пальца через SigLIP zero-shot классификацию.
        Возвращает True если score класса "a fingerprint" > 0.7
        """
        if _fp_classifier is None:
            return False
        try:
            results = _fp_classifier(path, candidate_labels=_fp_labels)
            for r in results:
                if r["label"] == "a fingerprint" and r["score"] > 0.7:
                    return True
        except Exception:
            pass
        return False


def detect_biometry(path: str) -> list:
        """
        Комбинированная детекция биометрии:
        - MediaPipe: лицо, глаза, силуэт тела
        - YOLOv8s: подпись
        - SigLIP: отпечаток пальца
        Возвращает список: ["лицо (2)", "глаза", "подпись"]
        """
        img = cv2.imread(path)
        if img is None:
            return []

        found = []
        rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)

        # Лицо + глаза (MediaPipe)
        try:
            with mp_face_detection.FaceDetection(
                model_selection=1, min_detection_confidence=0.5
            ) as detector:
                results = detector.process(rgb)
                if results.detections:
                    found.append(f"лицо ({len(results.detections)})")
                    for det in results.detections:
                        kp = det.location_data.relative_keypoints
                        if len(kp) >= 2:
                            found.append("глаза")
                            break
        except Exception:
            pass

        # Силуэт тела (MediaPipe Pose)
        try:
            with mp_pose.Pose(
                static_image_mode=True, min_detection_confidence=0.5
            ) as pose:
                if pose.process(rgb).pose_landmarks:
                    found.append("силуэт тела")
        except Exception:
            pass

        # Подпись (YOLOv8s) — принимает path, не gray
        try:
            if _detect_signature(path):
                found.append("подпись")
        except Exception:
            pass

        # Отпечаток пальца (SigLIP) — принимает path, не gray
        try:
            if _detect_fingerprint(path):
                found.append("отпечаток пальца")
        except Exception:
            pass

        return found

### Функция для парсинга одного файла

def worker_parse_file(file_data):
    """
    Автономный обработчик одного файла.
    Принимает: (индекс, путь_к_файлу, расширение)
    Возвращает: (индекс, извлеченный_текст)
    """

    idx, path, ext = file_data
    engine = choose_engine(ext) # функция выбора движка
    
    try:

        # Легкие форматы

        if engine == "json_engine":
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                data = json.load(f)
            # Используем вспомогательную функцию flatten_json (вынеси её тоже в корень)
            lines = flatten_json(data) 
            return idx, "\n".join(str(l) for l in lines).strip() or "ПУСТОЙ JSON"
            

        elif engine == "rtf_engine":
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    raw = f.read()
                # Предполагается наличие функции rtf_to_text
                text = rtf_to_text(raw) 
                return idx, text.strip() or "ПУСТОЙ RTF"
                
            except Exception as e:
                print(f"RTF не прочитан. Детали: {e}")
                

        elif engine == "doc_engine":
            try:
                # Пробуем через antiword (внешняя утилита)
                
                result = subprocess.run(
                    ["antiword", path], 
                    capture_output=True, 
                    text=True, 
                    timeout=30,
                    encoding='utf-8', # явно указываем кодировку для Windows
                    errors='ignore'
                )
                
                
                if result.returncode == 0 and result.stdout.strip():
                    return idx, result.stdout.strip()
                    
                else:
                    # Fallback: если antiword не выдал текст, читаем как бинарник
                    return idx, extract_binary(path)
                    
                    
            except FileNotFoundError:
                # antiword не установлен в системе — используем встроенный бинарный парсер
                return idx, extract_binary(path)
                
            except Exception as e:
                return idx, f"Ошибка DOC (бинарный парсинг): {e}"

        elif engine == "docx_engine":

            try:
                # Открываем документ
                doc = Document(path)
                # Извлекаем текст из всех параграфов
                text = "\n".join([p.text for p in doc.paragraphs])
                
                # Формируем результат
                result_text = text.strip() if text.strip() else "ПУСТОЙ DOCX"
                return idx, result_text
                
            except Exception as e:
                # Возвращаем описание ошибки вместо падения процесса
                return idx, f"Ошибка в чтении файла DOCx: {str(e)}"
        
        elif engine == "text_engine":

            try:

                with open(path, "r", encoding="utf-8", errors="ignore") as txt:
                    content = txt.read().strip()
                
                return idx, content if content else "ПУСТОЙ ТЕКСТОВЫЙ ФАЙЛ"
            except Exception as e:
                return idx, f"Ошибка в чтении текстового формата: {str(e)}"

        elif engine == "table_engine":

            try:
                import pandas as pd
                tbl_ext = os.path.splitext(path)[1].lower()
                tdf = None

                # 1. Чтение таблиц разных форматов
                if tbl_ext == ".csv":
                    for sep in [",", ";", "\t", "|"]:
                        try:
                            tdf = pd.read_csv(path, sep=sep, dtype=str, on_bad_lines="skip", encoding_errors="ignore")
                            if len(tdf.columns) > 1:
                                break
                        except:
                            continue
                    if tdf is None:
                        tdf = pd.read_csv(path, dtype=str, on_bad_lines="skip", encoding_errors="ignore")

                elif tbl_ext == ".tsv":
                    tdf = pd.read_csv(path, sep="\t", dtype=str, on_bad_lines="skip", encoding_errors="ignore")

                elif tbl_ext in (".xlsx", ".xls"):
                    eng = "openpyxl" if tbl_ext == ".xlsx" else "xlrd"
                    # Читаем все листы
                    sheets = pd.read_excel(path, sheet_name=None, dtype=str, engine=eng)
                    parts = []
                    for sname, sdf in sheets.items():
                        # Собираем заголовки и строки листа
                        lines = [" ".join(str(c) for c in sdf.columns)]
                        for _, r in sdf.iterrows():
                            row_t = " ".join(str(v) for v in r.values if pd.notna(v))
                            if row_t.strip():
                                lines.append(row_t)
                        if lines:
                            parts.append(f"[{sname}]\n" + "\n".join(lines))
                    
                    return idx, "\n\n".join(parts) if parts else "ПУСТАЯ ТАБЛИЦА"

                # 2. Обработка CSV/TSV после чтения
                if tdf is not None:
                    lines = [" ".join(str(c) for c in tdf.columns)]
                    for _, r in tdf.iterrows():
                        row_t = " ".join(str(v) for v in r.values if pd.notna(v))
                        if row_t.strip():
                            lines.append(row_t)
                    return idx, "\n".join(lines) if lines else "ПУСТАЯ ТАБЛИЦА"
                
                return idx, "НЕ УДАЛОСЬ ПРОЧИТАТЬ ТАБЛИЦУ"

            except Exception as e:
                return idx, f"Ошибка таблицы: {str(e)}"
        
        ### Тяжеловесные
                

        elif engine == "docx_engine":

            try:
                # Открываем документ
                doc = Document(path)
                # Извлекаем текст из всех параграфов
                text = "\n".join([p.text for p in doc.paragraphs])
                
                # Формируем результат
                result_text = text.strip() if text.strip() else "ПУСТОЙ DOCX"
                return idx, result_text
                
            except Exception as e:
                # Возвращаем описание ошибки вместо падения процесса
                return idx, f"Ошибка в чтении файла DOCx: {str(e)}"
        
        elif engine == "text_engine":

            try:

                with open(path, "r", encoding="utf-8", errors="ignore") as txt:
                    content = txt.read().strip()
                
                return idx, content if content else "ПУСТОЙ ТЕКСТОВЫЙ ФАЙЛ"
            except Exception as e:
                return idx, f"Ошибка в чтении текстового формата: {str(e)}"

        
        ### Тяжеловесные

        elif engine == "pdf_engine":
            try:
                # Порог количества символов.
                # Если на странице больше 500 символов, считаем, что это текстовый документ,
                # и биометрию (фото/подписи) искать не нужно.
                TEXT_THRESHOLD = 500

                with fitz.open(path) as doc:
                    
                    # --- Вспомогательная функция автоповорота ---
                    def fix_rotation(img):
                        try:
                            osd = pytesseract.image_to_osd(img, config='--psm 0', lang='rus+eng', output_type=pytesseract.Output.DICT)
                            angle = osd.get('rotate', 0)
                            if angle != 0:
                                img = img.rotate(-angle, expand=True)
                        except pytesseract.TesseractError:
                            pass
                        return img
                    # -------------------------------------------

                    # --- Основная функция обработки страницы ---
                    def process_page(page):
                        # 1. Пытаемся получить текст из PDF (без OCR)
                        # Используем "textdict" для более точного подсчета, если нужно, 
                        # но get_text() тоже подойдет для оценки объема.
                        raw_text = page.get_text()
                        text_len = len(raw_text.strip())
                        
                        # Если текста очень много, пропускаем тяжелые проверки
                        if text_len > TEXT_THRESHOLD:
                            # Возвращаем текст, биометрия пустая
                            return raw_text.strip(), ""

                        # 2. Если текста мало, это может быть скан или пустая страница.
                        # Делаем рендер, поворот и OCR.
                        pix = page.get_pixmap(matrix=fitz.Matrix(150/72, 150/72))
                        img = Image.open(io.BytesIO(pix.tobytes("png")))
                        
                        # Автоповорот
                        img = fix_rotation(img)
                        
                        # Распознаем текст
                        ocr_text = pytesseract.image_to_string(img, lang="rus+eng")
                        ocr_text_len = len(ocr_text.strip())
                        
                        # Обновляем итоговый текст (берем либо raw, либо OCR, в зависимости от того, что нашли)
                        # Логика: если raw был пустой, берем OCR. Если был небольшой, но OCR нашел больше - берем OCR.
                        final_text = raw_text.strip()
                        if ocr_text_len > text_len:
                            final_text = ocr_text.strip()
                        
                        bio_info = ""
                        
                        # 3. Проверяем биометрию ТОЛЬКО если текста мало.
                        # Если после OCR текста стало много (найден скрытый текст) — тоже пропускаем биометрию.
                        if len(final_text) < TEXT_THRESHOLD:
                            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                                img.save(tmp.name, format="PNG")
                                tmp_path = tmp.name
                            
                            bio = detect_biometry(tmp_path)
                            if bio:
                                bio_info = f"[БИОМЕТРИЯ стр.{page.number + 1}: {', '.join(bio)}]"
                            
                            os.unlink(tmp_path)
                        
                        return final_text, bio_info

                    # Запускаем обработку в параллельных потоках
                    with ThreadPoolExecutor(max_workers=8) as pool:
                        results = list(pool.map(process_page, doc))
                    
                    # Сборка результатов
                    final_text_parts = []
                    bio_results = []
                    
                    for text_part, bio_part in results:
                        if text_part:
                            final_text_parts.append(text_part)
                        if bio_part:
                            bio_results.append(bio_part)

                    full_text = "\n\n".join(final_text_parts)
                    if bio_results:
                        full_text += "\n" + "\n".join(bio_results)

                return idx, content if content else "ПУСТОЙ ПДФ ФАЙЛ"
            except Exception as e:
                return idx, f"Ошибка в чтении пдф формата: {str(e)}"

        elif engine == "whisper":
            try:
                import whisper
                # Загружаем модель внутри процесса. 
                # "base" весит около 150Мб, это допустимо для параллелизма.
                model = whisper.load_model("base") 

                # Загрузка и обработка аудио
                audio = whisper.load_audio(path)
                audio_segment = whisper.pad_or_trim(audio)

                # Детекция языка
                mel = whisper.log_mel_spectrogram(audio_segment).to(model.device)
                _, probs = model.detect_language(mel)

                detected_language = max(probs, key=probs.get)

                # Транскрибация
                result = model.transcribe(path, language=detected_language)
                
                text = result["text"].strip() if result else ""
                if text:
                    # Добавляем метку биометрии, как и в других воркерах
                    return idx, text + "\n[БИОМЕТРИЯ: образец голоса]"

                else:
                    return idx, "НИЧЕГО НЕ ИЗВЛЕЧЕНО (АУДИО ПУСТОЕ)"

            except Exception as e:
                return idx, f"Произошел сбой при извлечении аудиодорожки: {str(e)}"
                

        elif engine == "image_ocr":
            try:
                from PIL import Image
                # Открываем изображение
                img = Image.open(path)
                
                # Приводим к совместимому формату для Tesseract
                
                # Приводим к совместимому формату для Tesseract
                if img.mode not in ("L", "RGB"):
                    img = img.convert("RGB")
                
                # Извлекаем текст
                text = pytesseract.image_to_string(img, lang="rus+eng").strip()
                
                # Запускаем детекцию лиц, подписей и т.д.
                # Функция detect_biometry должна быть объявлена в корне файла
                # Извлекаем текст
                text = pytesseract.image_to_string(img, lang="rus+eng").strip()
                
                # Запускаем детекцию лиц, подписей и т.д.
                # Функция detect_biometry должна быть объявлена в корне файла
                bio = detect_biometry(path)
                
                
                if bio:
                    text += f"\n[БИОМЕТРИЯ: {', '.join(bio)}]"

                return idx, text if text else "OCR НЕ ИЗВЛЁК ТЕКСТ"
                
            except Exception as e:
                return idx, f"Ошибка OCR: {str(e)}"

        elif engine == "video_engine":
            try:
                import whisper
                import whisper
                results = []
                # Загружаем модель внутри процесса (лучше использовать маленькую для скорости)
                v_model = whisper.load_model("base")

                # 1. Извлекаем аудиодорожку через FFmpeg
                with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
                    tmp_audio = tmp.name
                
                subprocess.run([
                    FFMPEG_PATH, "-i", path,
                    "-vn", "-acodec", "pcm_s16le",
                    "-ar", "16000", "-ac", "1", "-y", tmp_audio
                ], capture_output=True, timeout=120)

                # 2. Транскрибация аудио
                if os.path.exists(tmp_audio) and os.path.getsize(tmp_audio) > 1000:
                    audio = whisper.load_audio(tmp_audio)
                    mel = whisper.log_mel_spectrogram(whisper.pad_or_trim(audio)).to(v_model.device)
                    _, probs = v_model.detect_language(mel)
                    mel = whisper.log_mel_spectrogram(whisper.pad_or_trim(audio)).to(v_model.device)
                    _, probs = v_model.detect_language(mel)
                    lang = max(probs, key=probs.get)
                    res = v_model.transcribe(tmp_audio, language=lang)
                    res = v_model.transcribe(tmp_audio, language=lang)
                    if res["text"].strip():
                        results.append(res["text"].strip())
                        results.append("[БИОМЕТРИЯ: образец голоса]")

                # 3. Нарезка кадров и OCR
                with tempfile.TemporaryDirectory() as tmpdir:
                    subprocess.run([
                        FFMPEG_PATH, "-i", path,
                        "-vf", "fps=1/10", "-q:v", "2", "-y",
                        os.path.join(tmpdir, "f_%04d.jpg")
                    ], capture_output=True, timeout=180)

                    prev_text = ""
                    for f_name in sorted(os.listdir(tmpdir)):
                        if not f_name.endswith(".jpg"):
                            continue
                        
                        frame_path = os.path.join(tmpdir, f_name)
                        img = Image.open(frame_path)
                        
                        # OCR кадра
                        t = pytesseract.image_to_string(img, lang="rus+eng").strip()
                        if t and t != prev_text:
                            results.append(t)
                            prev_text = t
                        
                        # Биометрия кадра
                        bio = detect_biometry(frame_path)
                        if bio:
                            results.append(f"[БИОМЕТРИЯ кадр {f_name}: {', '.join(bio)}]")

                return idx, "\n".join(results) if results else "ВИДЕО: ТЕКСТ НЕ ИЗВЛЕЧЁН"

            except Exception as e:
                return idx, f"Ошибка видео: {str(e)}"

        elif engine == "binary_engine":
            try:
                import polars
                # Читаем parquet файл

                temp = polars.read_parquet(path)
                
                # Приводим все колонки к строковому типу и объединяем в текст
                # Используем .sample или .head(1000), если файлы гигантские
                raw_text = " ".join(
                    temp.select(polars.all().cast(polars.Utf8))
                    .to_series()
                    .to_list()
                )
                
                return idx, raw_text if raw_text.strip() else "БИНАРНЫЙ ФАЙЛ ПУСТ"
            
            except Exception as e:
                return idx, f"Ошибка в чтении бинарника (Parquet). Детали: {e}"

        return idx, f"Для файла {ext} был не найден движок."
    except Exception as e:

        print(f"При парсинге произошла ошибка. Детали: {e}")
        return idx, f"При парсинге произошла ошибка. Детали: {e}", 

def parsing(df, update_callback=None):

    """
    
    """
    tasks = [(idx, row["Путь"], row["Расширение"]) for idx, row in df.iterrows()]
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(worker_parse_file, t): t for t in tasks}
        for i, f in enumerate(concurrent.futures.as_completed(futures)):
            res_idx, content = f.result()
            df.at[res_idx, "Содержание"] = content
            if update_callback:
                update_callback("Обработка...", i + 1, len(tasks))
    return df

### Обработка результатов парсинга

def seek_danger(df: pd.DataFrame) -> pd.DataFrame:
    """
    Функция принимает датафрейм с информацией из файлов, переведенной в формат строки.
    Функция производит проверку данных строк на наличие определенных ПДн.
    Каждое ПДн проверяется уникальным для него способом.
    Функция возвращает датафрейм с заполненной колонкой "Найденные ПДн".
    ПДн в колонке записаны в формате: ПДн(n1),ПДн(n2),...,ПДн(nN).
    Где ПДн - это некий ПДн, заданный в задании, n - количество найденных ПДн данного типа.
    """
    import re
    from collections import defaultdict
    from datetime import datetime
    
    # Инициализация Natasha и справочника БИК (один раз)
    _init_natasha()
    _init_bic_directory()
    
    # ========================================================================
    # 1. КЛЮЧЕВЫЕ СЛОВА ДЛЯ КОНТЕКСТА
    # ========================================================================
    BIRTH_KEYWORDS = [
        "родился", "родилась", "рождение", "рождён", "рождена",
        "дата рождения", "день рождения", "год рождения",
        "дата рожд", "г.р.", "г. рождения", "г р", "г рожд",
        "место рождения", "уроженец", "уроженка",
        "date of birth", "birth date", "born"
    ]
    
    ADDRESS_KEYWORDS = [
        "адрес регистрации", "место регистрации", "зарегистрирован по адресу",
        "адрес места жительства", "место жительства", "прописка",
        "проживает по адресу", "регистрация по адресу"
    ]
    
    # Маркеры организаций и юр.лиц для фильтрации
    ORGANIZATION_MARKERS = [
        "ооо", "оао", "зао", "пао", "ао", "ип", "нко", "гк", "гбу", "мбу",
        "фгуп", "муп", "компания", "организация", "учреждение", "предприятие",
        "корпорация", "банк", "группа", "холдинг", "фирма", "завод", "фабрика",
        "комбинат", "объединение", "союз", "ассоциация", "фонд", "партия",
        "департамент", "управление", "отдел", "служба", "агентство", "бюро",
        "центр", "институт", "университет", "академия", "министерство",
        "ведомство", "комитет", "совет", "администрация", "правительство",
        "прокуратура", "суд", "инспекция", "казначейство", "лицей", "гимназия",
        "школа", "колледж", "техникум", "училище", "больница", "поликлиника",
        "аптека", "магазин", "торговый", "строительная", "производственная",
        "транспортная", "страховая", "управляющая", "ресурсоснабжающая",
    ]
    
    # ========================================================================
    # 2. ПАТТЕРНЫ ДЛЯ ПОИСКА
    # ========================================================================
    patterns = {
        "Телефон": r"(?:\+7|8)[\s\(-]?\d{3}[\s\)-]?\d{3}[\s-]?\d{2}[\s-]?\d{2}\b",
        "Email": r"[\w\.-]+@[\w\.-]+\.\w+",
        "СНИЛС": r"\d{3}-\d{3}-\d{3}\s\d{2}\b",
        "ИНН": r"\b\d{10}\b|\b\d{12}\b",
        "Паспорт": r"\b(?:паспорт|серия|выдан|кем\s+выдан|паспортные\s+данные)\b\s*:?\s*\d{2}\s?\d{2}\s?\d{6}\b",
        "Водительское удостоверение": r"[АВЕКМНОРСТУХ]{2}\d{6}\b",
        "MRZ": r"[A-Z0-9<]{44,88}",
        "Банковская карта": r"\b(?:\d{4}[- ]?){3}\d{4}\b",
        "CVV": r"(?:cvv|cvc|код\s+безопасности|код\s+карты|cvv2/cvc2|код\s+cvv|cvv\s+код)\s*:?\s*\d{3}\b",
        "Заработная плата": r"\b(?:зарплата|оклад|доход|зп|заработная\s+плата|ежемесячный\s+доход|среднемесячный\s+доход)\b\s*:?\s*\d+",
        "Медицина": r"\b(?:диагноз|заболевание|болезнь|анамнез|жалобы|лечение|терапия|мкб-\d+|рецепт|назначено|таблетки|дозировка|больница|поликлиника|медцентр|клиника|врач|медицинская\s+карта)\b",
        "Полис ОМС": r"\b(?:полис|омс|страховой\s+полис|медицинский\s+полис)\b\s*:?\s*\b\d{16}\b",
        "Биометрия: лицо": r"\[БИОМЕТРИЯ[^\]]*лицо",
        "Биометрия: глаза": r"\[БИОМЕТРИЯ[^\]]*глаза",
        "Биометрия: силуэт": r"\[БИОМЕТРИЯ[^\]]*силуэт",
        "Биометрия: подпись": r"\[БИОМЕТРИЯ[^\]]*подпись",
        "Биометрия: отпечаток": r"\[БИОМЕТРИЯ[^\]]*отпечаток",
        "Биометрия: голос": r"\[БИОМЕТРИЯ[^\]]*голос",
    }
    
    # ========================================================================
    # 3. СПИСКИ ДЛЯ ПРОВЕРКИ
    # ========================================================================
    RACE_VALUES = {
        "европеоид", "европеоидная", "европеоидной", "европеоидную", "европеоидный",
        "кавказоид", "кавказоидная", "кавказоидной", "кавказоидную", "кавказоидный",
        "монголоид", "монголоидная", "монголоидной", "монголоидную", "монголоидный",
        "негроид", "негроидная", "негроидной", "негроидную", "негроидный",
        "австралоид", "австралоидная", "австралоидной", "австралоидную", "австралоидный",
    }
    
    NATIONALITIES = {
        "русский", "русская", "татарин", "татарка", "украинец", "украинка",
        "башкир", "башкирка", "чуваш", "чувашка", "чеченец", "чеченка",
        "армянин", "армянка", "азербайджанец", "азербайджанка", "казах", "казашка",
        "белорус", "белоруска", "узбек", "узбечка", "таджик", "таджичка",
        "киргиз", "киргизка", "грузин", "грузинка", "молдаванин", "молдаванка",
        "немец", "немка", "еврей", "еврейка", "кореец", "кореянка", "китаец", "китаянка",
    }
    
    RELIGIONS = {
        "православие", "христианство", "ислам", "буддизм", "иудаизм",
        "католицизм", "протестантизм", "индуизм", "атеист", "агностик"
    }
    
    POLITICAL_VIEWS = {
        "коммунист", "либерал", "консерватор", "социал-демократ",
        "националист", "анархист", "социалист", "демократ", "монархист"
    }
    
    CRIMINAL_WORDS = {
        "судимость", "судим", "осужден", "привлекался", "несудим"
    }
    
    VALID_OPERATOR_CODES = {
        "900", "901", "902", "903", "904", "905", "906", "908", "909",
        "910", "911", "912", "913", "914", "915", "916", "917", "918", "919",
        "920", "921", "922", "923", "924", "925", "926", "927", "928", "929",
        "930", "931", "932", "933", "934", "936", "937", "938", "939",
        "941", "942", "949",
        "950", "951", "952", "953", "954", "955", "958", "959",
        "960", "961", "962", "963", "964", "965", "966", "967", "968", "969",
        "970", "971", "977", "978", "979",
        "980", "981", "982", "983", "984", "985", "986", "987", "988", "989",
        "990", "991", "992", "993", "994", "995", "996", "997", "999"
    }
    
    # ========================================================================
    # 4. ФУНКЦИИ ВАЛИДАЦИИ
    # ========================================================================
    
    def is_valid_snils(snils_str: str) -> bool:
        digits = re.sub(r'\D', '', snils_str)
        if len(digits) != 11 or digits == "00000000000":
            return False
        total = sum(int(digits[i]) * (9 - i) for i in range(9))
        check = total % 101
        if check == 100:
            check = 0
        return check == int(digits[9:])
    
    def is_valid_inn(inn_str: str) -> bool:
        if not inn_str.isdigit():
            return False
        inn_len = len(inn_str)
        if inn_len == 10:
            coeffs = [2, 4, 10, 3, 5, 9, 4, 6, 8]
            total = sum(int(inn_str[i]) * coeffs[i] for i in range(9))
            check = total % 11
            if check == 10:
                check = 0
            return check == int(inn_str[9])
        elif inn_len == 12:
            coeffs1 = [7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
            total1 = sum(int(inn_str[i]) * coeffs1[i] for i in range(10))
            check1 = total1 % 11
            if check1 == 10:
                check1 = 0
            if check1 != int(inn_str[10]):
                return False
            coeffs2 = [3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
            total2 = sum(int(inn_str[i]) * coeffs2[i] for i in range(11))
            check2 = total2 % 11
            if check2 == 10:
                check2 = 0
            return check2 == int(inn_str[11])
        return False
    
    def is_valid_phone(phone_str: str) -> bool:
        digits = re.sub(r'\D', '', phone_str)
        if len(digits) != 11:
            return False
        if digits[0] not in ['7', '8']:
            return False
        operator_code = digits[1:4]
        return operator_code in VALID_OPERATOR_CODES
    
    def is_valid_driver_license(license_str: str) -> bool:
        VALID_LETTERS = set("АВЕКМНОРСТУХ")
        if len(license_str) != 8:
            return False
        for i in range(2):
            if license_str[i] not in VALID_LETTERS:
                return False
        if not license_str[2:].isdigit():
            return False
        return True
    
    def is_valid_mrz(mrz_str: str) -> bool:
        mrz_clean = mrz_str.replace(' ', '').replace('\n', '').replace('\r', '')
        if len(mrz_clean) not in [44, 88]:
            return False
        if not re.match(r'^[A-Z0-9<]+$', mrz_clean):
            return False
        return True
    
    def is_valid_card_number(card_str: str) -> bool:
        digits = re.sub(r'[\s-]', '', card_str)
        if not digits.isdigit() or len(digits) != 16:
            return False
        total = 0
        for i, digit in enumerate(digits):
            num = int(digit)
            if i % 2 == 0:
                num *= 2
                if num > 9:
                    num -= 9
            total += num
        return total % 10 == 0
    
    def is_valid_bic(bic_str: str) -> bool:
        if not bic_str.isdigit() or len(bic_str) != 9:
            return False
        if not bic_str.startswith('04'):
            return False
        return bic_str in _VALID_BICS
    
    def is_valid_bank_account(account_str: str, bic_str: str = None) -> tuple:
        digits = re.sub(r'\D', '', account_str)
        if len(digits) != 20:
            return (False, "не 20 цифр")
        
        if bic_str and bic_str in _VALID_BICS:
            bank_code = bic_str[-3:]
            check_string = bank_code + digits
            weights = [7, 1, 3, 7, 1, 3, 7, 1, 3, 7, 1, 3, 7, 1, 3, 7, 1, 3, 7, 1, 7, 1, 3]
            total = 0
            for i, digit in enumerate(check_string):
                total += int(digit) * weights[i]
            if total % 10 == 0:
                return (True, "OK")
            else:
                return (False, "контрольная сумма не совпадает")
        
        return (False, "нет БИК для проверки")
    
    def is_valid_cvv(cvv_str: str) -> bool:
        digits = re.sub(r'\D', '', cvv_str)
        return len(digits) == 3 and digits.isdigit()
    
    def is_valid_oms_policy(policy_str: str) -> bool:
        digits = re.sub(r'\D', '', policy_str)
        if len(digits) != 16:
            return False
        weights = [1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3]
        total = sum(int(d) * w for d, w in zip(digits, weights))
        return total % 10 == 0
    
    # ========================================================================
    # 5. ФУНКЦИЯ ПОИСКА ФИО ЧЕРЕЗ NATASHA
    # ========================================================================
    def find_person_names(text: str, file_path: str = "") -> int:
        """
        Ищет ФИО в тексте с помощью Natasha.
        Фильтрует организации, города, служебные пометки.
        Объединяет разрозненные части одного ФИО.
        Удаляет дубликаты.
        Возвращает количество уникальных ФИО.
        """
        if not _NATASHA_INITIALIZED or _NATASHA_NER_TAGGER is None:
            return 0
        
        try:
            from natasha import Doc
            
            # Заменяем переносы строк и табуляции на пробелы
            normalized_text = re.sub(r'[\n\r\t]+', ' ', text)
            normalized_text = re.sub(r'\s+', ' ', normalized_text)
            
            doc = Doc(normalized_text)
            doc.segment(_NATASHA_SEGMENTER)
            doc.tag_ner(_NATASHA_NER_TAGGER)
            
            raw_names = []
            
            # Слова и паттерны, которые НЕ являются ФИО
            NOT_NAME_PATTERNS = [
                r'^[А-ЯЁ]\.\s*[А-ЯЁ]\.$',  # И.О.
                r'^[А-ЯЁ]\.[А-ЯЁ]\.$',     # И.О.
                r'^м\.п\.?$',              # М.П.
                r'^подпись',               # Подпись
                r'^расшифровка',           # Расшифровка
                r'^фамилия$',              # Фамилия
                r'^имя$',                  # Имя
                r'^отчество$',             # Отчество
                r'^пол\s',                 # Пол мужской/женский
                r'^г\.\s',                 # г. Город
                r'^город\s',               # Город
            ]
            
            # Слова, которые указывают что это не ФИО
            NOT_NAME_WORDS = {
                'инженер', 'конструктор', 'менеджер', 'директор', 'руководитель',
                'специалист', 'администратор', 'оператор', 'техник', 'программист',
                'аналитик', 'бухгалтер', 'экономист', 'юрист', 'секретарь',
                'solidworks', 'autodesk', 'inventor', 'microsoft', 'google',
                'подпись', 'расшифровка', 'фамилия', 'отчество',
            }
            
            if doc is not None and hasattr(doc, 'spans') and doc.spans is not None:
                for span in doc.spans:
                    if span is not None and hasattr(span, 'type') and span.type == "PER":
                        name_text = normalized_text[span.start:span.stop].strip()
                        name_lower = name_text.lower()
                        
                        # Проверяем на паттерны "не ФИО"
                        is_not_name = False
                        for pattern in NOT_NAME_PATTERNS:
                            if re.match(pattern, name_lower, re.IGNORECASE):
                                is_not_name = True
                                break
                        
                        if is_not_name:
                            print(f"[ФИО DEBUG]   -> Отфильтровано по паттерну: '{name_text}'")
                            continue
                        
                        # Проверяем на слова "не ФИО"
                        words = name_lower.split()
                        has_not_name_word = any(word in NOT_NAME_WORDS for word in words)
                        if has_not_name_word:
                            print(f"[ФИО DEBUG]   -> Отфильтровано по словарю: '{name_text}'")
                            continue
                        
                        # Проверяем что ФИО содержит хотя бы одно слово с заглавной буквы (не все капсом)
                        # Это отсеет "ПОЛ МУЖСКОЙ", "М.П." и т.д.
                        if name_text.isupper() and len(words) <= 2:
                            # Если всё капсом и короткое - скорее всего не ФИО
                            print(f"[ФИО DEBUG]   -> Отфильтровано (всё капсом): '{name_text}'")
                            continue
                        
                        # Проверяем контекст на организации и юр.лица
                        context_start = max(0, span.start - 150)
                        context_end = min(len(normalized_text), span.stop + 50)
                        context = normalized_text[context_start:context_end].lower()
                        
                        is_org = False
                        for marker in ORGANIZATION_MARKERS:
                            if marker in context:
                                is_org = True
                                break
                        
                        if is_org:
                            print(f"[ФИО DEBUG]   -> Отфильтровано (организация в контексте): '{name_text}'")
                            continue
                        
                        raw_names.append({
                            'text': name_text,
                            'start': span.start,
                            'end': span.stop,
                            'words': set(name_text.lower().split())
                        })
            
            # Логирование сырых найденных PER
            if raw_names:
                print(f"\n[ФИО DEBUG] Файл: {file_path}")
                print(f"[ФИО DEBUG] Сырые PER от Natasha после фильтрации ({len(raw_names)} шт.):")
                for i, name in enumerate(raw_names):
                    print(f"  {i+1}. '{name['text']}' (позиция: {name['start']}-{name['end']})")
            
            if not raw_names:
                print(f"[ФИО DEBUG] Файл: {file_path}")
                print(f"[ФИО DEBUG] ФИО не найдены\n")
                return 0
            
            # Группируем имена, которые являются частями одного ФИО
            merged_names = []
            used = set()
            
            for i, name1 in enumerate(raw_names):
                if i in used:
                    continue
                    
                merged_text = name1['text']
                merged_start = name1['start']
                merged_end = name1['end']
                merged_words = name1['words'].copy()
                
                # Ищем другие части этого же ФИО
                for j, name2 in enumerate(raw_names[i+1:], i+1):
                    if j in used:
                        continue
                    
                    # Если имена находятся близко (в пределах 100 символов)
                    distance = min(abs(name2['start'] - merged_end), abs(merged_start - name2['end']))
                    
                    if distance < 100:
                        # Проверяем, не пересекаются ли слова
                        if not merged_words.intersection(name2['words']):
                            # Объединяем тексты
                            if name2['start'] < merged_start:
                                merged_text = name2['text'] + ' ' + merged_text
                                merged_start = name2['start']
                            else:
                                merged_text = merged_text + ' ' + name2['text']
                            
                            merged_end = max(merged_end, name2['end'])
                            merged_words.update(name2['words'])
                            used.add(j)
                            print(f"[ФИО DEBUG]   -> Объединено с '{name2['text']}' (расстояние: {distance})")
                
                used.add(i)
                merged_names.append(merged_text)
            
            if merged_names:
                print(f"[ФИО DEBUG] После объединения ({len(merged_names)} шт.):")
                for i, name in enumerate(merged_names):
                    print(f"  {i+1}. '{name}'")
            
            # Удаляем дубликаты и фильтруем одиночные слова
            unique_names = set()
            for name in merged_names:
                normalized_name = ' '.join(name.lower().split())
                words = normalized_name.split()
                
                # Оставляем только если минимум 2 слова
                if len(words) >= 2:
                    # Дополнительная проверка: слова должны быть кириллицей
                    if all(re.match(r'^[а-яё-]+$', word) for word in words):
                        unique_names.add(normalized_name)
                    else:
                        print(f"[ФИО DEBUG]   -> Отфильтровано (не кириллица): '{name}'")
                else:
                    print(f"[ФИО DEBUG]   -> Отфильтровано (одно слово): '{name}'")
            
            # Финальная проверка: удаляем имена, которые являются частью более полных
            final_names = set()
            names_list = sorted(list(unique_names), key=len, reverse=True)
            
            for name in names_list:
                is_subset = False
                for longer_name in final_names:
                    if name in longer_name:
                        is_subset = True
                        print(f"[ФИО DEBUG]   -> Удалено '{name}' (часть '{longer_name}')")
                        break
                if not is_subset:
                    final_names.add(name)
            
            print(f"[ФИО DEBUG] Итоговые ФИО ({len(final_names)} шт.):")
            for i, name in enumerate(sorted(final_names)):
                print(f"  {i+1}. '{name}'")
            print()
            
            return len(final_names)
            
        except Exception as e:
            print(f"Ошибка при поиске ФИО: {e}")
            return 0
    
    # ========================================================================
    # 6. ФУНКЦИЯ ПРОВЕРКИ ДАТЫ РОЖДЕНИЯ
    # ========================================================================
    def find_birth_dates(text: str) -> list:
        if not _NATASHA_INITIALIZED or _NATASHA_DATES_EXTRACTOR is None:
            return []
        
        all_dates = []
        try:
            for date_match in _NATASHA_DATES_EXTRACTOR(text):
                year = None
                date_text = ""
                date_start = 0
                date_end = 0
                
                if hasattr(date_match, 'fact'):
                    fact = date_match.fact
                    year = getattr(fact, 'year', None)
                    date_text = getattr(fact, 'text', '')
                    date_start = getattr(date_match, 'start', 0)
                    date_end = getattr(date_match, 'stop', 0)
                elif hasattr(date_match, 'year'):
                    year = date_match.year
                    date_text = getattr(date_match, 'text', '')
                    date_start = getattr(date_match, 'start', 0)
                    date_end = getattr(date_match, 'stop', 0)
                else:
                    continue
                
                if not year or year < 1900 or year > 2026:
                    continue
                
                if not date_text and date_start < date_end:
                    date_text = text[date_start:date_end]
                
                if not date_text:
                    continue
                
                try:
                    date_obj = datetime(year, 1, 1)
                except:
                    date_obj = datetime(year, 1, 1)
                
                all_dates.append({
                    'text': date_text,
                    'date_obj': date_obj,
                    'start': date_start,
                    'end': date_end
                })
        except Exception as e:
            return []
        
        if not all_dates:
            return []
        
        dates_with_context = []
        for date_info in all_dates:
            date_start = date_info['start']
            date_end = date_info['end']
            
            context_start = max(0, date_start - 300)
            context_end = min(len(text), date_end + 300)
            context = text[context_start:context_end].lower()
            context_clean = ' '.join(context.split())
            
            if any(keyword in context_clean for keyword in BIRTH_KEYWORDS):
                dates_with_context.append(date_info)
        
        if not dates_with_context:
            return []
        
        if len(dates_with_context) > 1:
            dates_with_context.sort(key=lambda x: x['date_obj'])
        
        return [dates_with_context[0]['text']]
    
    # ========================================================================
    # 7. ФУНКЦИЯ ПОИСКА ЛОКАЦИЙ ПО КЛЮЧЕВЫМ СЛОВАМ
    # ========================================================================
    def find_locations_by_keywords(text: str, keywords: list) -> list:
        if not _NATASHA_INITIALIZED or _NATASHA_NER_TAGGER is None:
            return []
        
        try:
            from natasha import Doc
            
            doc = Doc(text)
            doc.segment(_NATASHA_SEGMENTER)
            doc.tag_ner(_NATASHA_NER_TAGGER)
            
            locations = []
            if doc is not None and hasattr(doc, 'spans') and doc.spans is not None:
                for span in doc.spans:
                    if span is not None and hasattr(span, 'type') and span.type == "LOC":
                        try:
                            locations.append({
                                'text': text[span.start:span.stop],
                                'start': span.start,
                                'stop': span.stop
                            })
                        except (IndexError, TypeError):
                            continue
            
            if not locations:
                return []
            
            keyword_positions = []
            text_lower = text.lower()
            
            for keyword in keywords:
                start = 0
                while True:
                    pos = text_lower.find(keyword, start)
                    if pos == -1:
                        break
                    keyword_positions.append({
                        'keyword': keyword,
                        'position': pos,
                        'end': pos + len(keyword)
                    })
                    start = pos + 1
            
            if not keyword_positions:
                return []
            
            found_places = []
            for kw in keyword_positions:
                kw_pos = kw['position']
                kw_end = kw['end']
                
                best_match = None
                best_distance = float('inf')
                
                for loc in locations:
                    loc_start = loc['start']
                    loc_end = loc['stop']
                    
                    if loc_end < kw_pos:
                        distance = kw_pos - loc_end
                    elif loc_start > kw_end:
                        distance = loc_start - kw_end
                    else:
                        distance = 0
                    
                    if distance < best_distance:
                        best_distance = distance
                        best_match = loc['text']
                
                if best_match and best_distance < 500:
                    found_places.append(best_match)
            
            seen = set()
            unique_places = []
            for place in found_places:
                if place not in seen:
                    seen.add(place)
                    unique_places.append(place)
            
            return unique_places
            
        except Exception as e:
            print(f"Ошибка при поиске локаций: {e}")
            return []
    
    # ========================================================================
    # 8. ОСНОВНОЙ ЦИКЛ
    # ========================================================================
    for idx, row in df.iterrows():
        if str(row["Содержание"]).split(" ")[0] == "Ошибка":
            df.at[idx, "Найденные ПДн"] = "Нет никаких нарушений"
            continue
        
        info = str(row["Содержание"])
        info_lower = info.lower()
        pd_count = defaultdict(int)
        
        # Предварительный поиск всех БИК в тексте
        found_bics = []
        for match in re.finditer(r'\b\d{9}\b', info):
            bic_candidate = match.group()
            if is_valid_bic(bic_candidate):
                found_bics.append({
                    'bic': bic_candidate,
                    'position': match.start(),
                    'end': match.end()
                })
        
        # ====================================================================
        # 8.1. ПОИСК ПО РЕГУЛЯРНЫМ ВЫРАЖЕНИЯМ
        # ====================================================================
        for pattern_name, pattern in patterns.items():
            text_to_search = info_lower if pattern_name in ["Медицина", "Паспорт", "CVV", "Полис ОМС"] else info
            
            for match in re.finditer(pattern, text_to_search, re.IGNORECASE):
                match_text = match.group()
                start_pos = match.start()
                
                valid = True
                
                if pattern_name == "СНИЛС":
                    valid = is_valid_snils(match_text)
                elif pattern_name == "ИНН":
                    valid = is_valid_inn(match_text)
                elif pattern_name == "Телефон":
                    valid = is_valid_phone(match_text)
                elif pattern_name == "Водительское удостоверение":
                    valid = is_valid_driver_license(match_text)
                elif pattern_name == "MRZ":
                    valid = is_valid_mrz(match_text)
                elif pattern_name == "Банковская карта":
                    valid = is_valid_card_number(match_text)
                elif pattern_name == "БИК":
                    valid = is_valid_bic(match_text)
                elif pattern_name == "Банковский счет":
                    nearby_bic = None
                    for bic_info in found_bics:
                        if abs(bic_info['position'] - start_pos) < 200:
                            nearby_bic = bic_info['bic']
                            break
                    valid, _ = is_valid_bank_account(match_text, nearby_bic)
                elif pattern_name == "CVV":
                    valid = is_valid_cvv(match_text)
                elif pattern_name == "Полис ОМС":
                    valid = is_valid_oms_policy(match_text)
                
                if valid:
                    pd_count[pattern_name] += 1
        
        # ====================================================================
        # 8.2. ПОИСК ФИО
        # ====================================================================
        names_count = find_person_names(info)
        if names_count > 0:
            pd_count["ФИО"] = names_count
        
        # ====================================================================
        # 8.3. ПОИСК ДАТЫ РОЖДЕНИЯ
        # ====================================================================
        birth_dates = find_birth_dates(info)
        if birth_dates:
            pd_count["Дата рождения"] = len(birth_dates)
        
        # ====================================================================
        # 8.4. ПОИСК МЕСТА РОЖДЕНИЯ
        # ====================================================================
        birth_keywords = ["родился", "родилась", "место рождения", "уроженец", "уроженка"]
        birth_places = find_locations_by_keywords(info, birth_keywords)
        if birth_places:
            pd_count["Место рождения"] = len(birth_places)
        
        # ====================================================================
        # 8.5. ПОИСК АДРЕСА РЕГИСТРАЦИИ
        # ====================================================================
        address_places = find_locations_by_keywords(info, ADDRESS_KEYWORDS)
        if address_places:
            pd_count["Адрес регистрации"] = len(address_places)
        
        # ====================================================================
        # 8.6. ПОИСК ПО NATASHA (словари)
        # ====================================================================
        if _NATASHA_INITIALIZED and _NATASHA_NER_TAGGER is not None:
            try:
                from natasha import Doc
                
                doc = Doc(info)
                doc.segment(_NATASHA_SEGMENTER)
                doc.tag_ner(_NATASHA_NER_TAGGER)
                
                if doc is not None and _NATASHA_MORPH_VOCAB is not None:
                    if hasattr(doc, 'tokens') and doc.tokens is not None:
                        for token in doc.tokens:
                            if token is not None:
                                try:
                                    token.lemmatize(_NATASHA_MORPH_VOCAB)
                                    lemma = token.lemma.lower() if token.lemma else ""
                                    word = token.text.lower() if token.text else ""
                                    
                                    if lemma in RACE_VALUES or word in RACE_VALUES:
                                        pd_count["Раса"] += 1
                                    elif lemma in NATIONALITIES or word in NATIONALITIES:
                                        pd_count["Национальность"] += 1
                                    elif lemma in RELIGIONS or word in RELIGIONS:
                                        pd_count["Религиозные убеждения"] += 1
                                    elif lemma in POLITICAL_VIEWS or word in POLITICAL_VIEWS:
                                        pd_count["Политические убеждения"] += 1
                                    elif lemma in CRIMINAL_WORDS or word in CRIMINAL_WORDS:
                                        pd_count["Судимость"] += 1
                                except Exception:
                                    continue
            except Exception as e:
                print(f"Ошибка при обработке Natasha: {e}")
        
        # ====================================================================
        # 8.7. ФОРМИРОВАНИЕ РЕЗУЛЬТАТА
        # ====================================================================
        if pd_count:
            result_parts = [f"{pd_type}({count})" for pd_type, count in pd_count.items()]
            df.at[idx, "Найденные ПДн"] = ",".join(result_parts)
        else:
            df.at[idx, "Найденные ПДн"] = "Нет никаких нарушений"
    
    return df

def categories(df: pd.DataFrame) -> pd.DataFrame:
    """
    Функция принимает на вход датафрейм и по колонке "Найденные ПДн"
    распределяет их по категориям и записывает результат в колонку "Категории".
    """
    
    import re
    from collections import defaultdict
    
    # ========================================================================
    # 1. ОБЫЧНЫЕ ПЕРСОНАЛЬНЫЕ ДАННЫЕ
    # ========================================================================
    COMMON_PD = {
        "ФИО",
        "Телефон",
        "Email",
        "Дата рождения",
        "Место рождения",
        "Адрес регистрации",
    }
    
    # ========================================================================
    # 2. ГОСУДАРСТВЕННЫЕ ИДЕНТИФИКАТОРЫ
    # ========================================================================
    GOV_ID = {
        "Паспорт",
        "СНИЛС",
        "ИНН",
        "Водительское удостоверение",
        "MRZ",
        "Полис ОМС",
    }
    
    # ========================================================================
    # 3. ПЛАТЕЖНАЯ ИНФОРМАЦИЯ
    # ========================================================================
    PAYMENT_INFO = {
        "Банковская карта",
        "Банковский счет",
        "БИК",
        "CVV",
    }
    
    # ========================================================================
    # 4. БИОМЕТРИЧЕСКИЕ ДАННЫЕ
    # ========================================================================
    BIOMETRIC_DATA = {
        "Биометрия: лицо",
        "Биометрия: глаза",
        "Биометрия: силуэт",
        "Биометрия: подпись",
        "Биометрия: отпечаток",
        "Биометрия: голос",
    }
    
    # ========================================================================
    # 5. СПЕЦИАЛЬНЫЕ КАТЕГОРИИ ПДн
    # ========================================================================
    SPECIAL_CATEGORIES = {
        "Медицина",
        "Национальность",
        "Раса",
        "Религиозные убеждения",
        "Политические убеждения",
        "Судимость",
    }
    
    # ========================================================================
    # 6. ФИНАНСОВЫЕ ДАННЫЕ
    # ========================================================================
    FINANCIAL_DATA = {
        "Заработная плата",
    }
    
    # ========================================================================
    # 7. МАППИНГ: тип ПДн -> категория
    # ========================================================================
    PD_TO_CATEGORY = {}
    
    for pd_type in COMMON_PD:
        PD_TO_CATEGORY[pd_type] = "Обычные персональные данные"
    
    for pd_type in GOV_ID:
        PD_TO_CATEGORY[pd_type] = "Государственные идентификаторы"
    
    for pd_type in PAYMENT_INFO:
        PD_TO_CATEGORY[pd_type] = "Платежная информация"
    
    for pd_type in BIOMETRIC_DATA:
        PD_TO_CATEGORY[pd_type] = "Биометрические данные"
    
    for pd_type in SPECIAL_CATEGORIES:
        PD_TO_CATEGORY[pd_type] = "Специальные категории ПДн"
    
    for pd_type in FINANCIAL_DATA:
        PD_TO_CATEGORY[pd_type] = "Финансовые данные"
    
    # ========================================================================
    # 8. ОСНОВНОЙ ЦИКЛ
    # ========================================================================
    for idx, row in df.iterrows():
        found_pdns_str = row["Найденные ПДн"]
        
        if found_pdns_str == "Нет никаких нарушений":
            df.at[idx, "Категории"] = "Нет нарушений"
            continue
        
        category_counts = defaultdict(int)
        
        for item in found_pdns_str.split(","):
            item = item.strip()
            if not item:
                continue
            
            # ИСПРАВЛЕНО: добавлены латинские буквы A-Za-z
            match = re.match(r'^([A-Za-zА-Яа-я\s:]+)\((\d+)\)$', item)
            if match:
                pd_type = match.group(1).strip()
                count = int(match.group(2))
                
                category = PD_TO_CATEGORY.get(pd_type)
                if category:
                    category_counts[category] += count
                else:
                    # Пробуем найти без учёта регистра
                    pd_type_lower = pd_type.lower()
                    found = False
                    for key in PD_TO_CATEGORY.keys():
                        if key.lower() == pd_type_lower:
                            category = PD_TO_CATEGORY[key]
                            category_counts[category] += count
                            found = True
                            break
                    
                    if not found:
                        category_counts["Неизвестная категория"] += count
        
        if category_counts:
            result_parts = [f"{category}({count})" for category, count in sorted(category_counts.items())]
            df.at[idx, "Категории"] = ",".join(result_parts)
        else:
            df.at[idx, "Категории"] = "Неизвестная категория"
    
    return df

def evaluate_violations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Функция принимает на вход датафрейм и по колонке "Категории"
    определяет требуемый уровень защищенности (УЗ) от 1 до 4.
    
    Уровни защищенности (Приказ ФСТЭК России №21):
    - УЗ-1: Наличие специальных категорий ПДн или биометрических данных (высокий риск)
    - УЗ-2: Наличие платежной информации или государственных идентификаторов в больших объемах (более 3)
    - УЗ-3: Наличие государственных идентификаторов в небольших объемах (от 1 до 3 включительно) 
            или обычных ПДн в больших объемах (более 3)
    - УЗ-4: Наличие только обычных ПДн в небольших объемах (от 1 до 3 включительно)
    - 0: Нет нарушений
    """
    
    import re
    from collections import defaultdict
    
    # ========================================================================
    # 1. ОПРЕДЕЛЕНИЕ КАТЕГОРИЙ
    # ========================================================================
    
    HIGH_RISK_CATEGORIES = {
        "Специальные категории ПДн",
        "Биометрические данные",
    }
    
    PAYMENT_CATEGORIES = {
        "Платежная информация",
    }
    
    GOV_ID_CATEGORIES = {
        "Государственные идентификаторы",
    }
    
    COMMON_PD_CATEGORIES = {
        "Обычные персональные данные",
        "Финансовые данные",
    }
    
    # ========================================================================
    # 2. ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ
    # ========================================================================
    def parse_categories(categories_str: str) -> dict:
        result = defaultdict(int)
        
        if not categories_str or categories_str == "Нет нарушений":
            return result
        
        for item in categories_str.split(","):
            item = item.strip()
            if not item:
                continue
            
            match = re.match(r'^([А-Яа-я\s]+)\((\d+)\)$', item)
            if match:
                category = match.group(1).strip()
                count = int(match.group(2))
                result[category] = count
        
        return result
    
    # ========================================================================
    # 3. ОСНОВНОЙ ЦИКЛ
    # ========================================================================
    for idx, row in df.iterrows():
        categories_str = row["Категории"]
        
        # Если нет нарушений
        if categories_str == "Нет нарушений":
            df.at[idx, "Требуемый УЗ"] = 0 
            continue
        
        categories_counts = parse_categories(categories_str)
        
        if not categories_counts:
            df.at[idx, "Требуемый УЗ"] = 0 
            continue
        
        # ====================================================================
        # УЗ-1: Специальные категории или биометрия
        # ====================================================================
        has_high_risk = False
        for category in HIGH_RISK_CATEGORIES:
            if category in categories_counts and categories_counts[category] > 0:
                has_high_risk = True
                break
        
        if has_high_risk:
            df.at[idx, "Требуемый УЗ"] = 1 
            continue
        
        # ====================================================================
        # УЗ-2: Платежная информация ИЛИ гос. идентификаторы > 3
        # ====================================================================
        has_payment = False
        for category in PAYMENT_CATEGORIES:
            if category in categories_counts and categories_counts[category] > 0:
                has_payment = True
                break
        
        if has_payment:
            df.at[idx, "Требуемый УЗ"] = 2
            continue
        
        gov_id_count = 0
        for category in GOV_ID_CATEGORIES:
            gov_id_count += categories_counts.get(category, 0)
        
        if gov_id_count > 3:
            df.at[idx, "Требуемый УЗ"] = 2
            continue
        
        # ====================================================================
        # УЗ-3: Гос. идентификаторы 1-3 ИЛИ обычные ПДн > 3
        # ====================================================================
        if 1 <= gov_id_count <= 3:
            df.at[idx, "Требуемый УЗ"] = 3
            continue
        
        common_pd_count = 0
        for category in COMMON_PD_CATEGORIES:
            common_pd_count += categories_counts.get(category, 0)
        
        if common_pd_count > 3:
            df.at[idx, "Требуемый УЗ"] = 3
            continue
        
        # ====================================================================
        # УЗ-4: Только обычные ПДн 1-3
        # ====================================================================
        if 1 <= common_pd_count <= 3:
            df.at[idx, "Требуемый УЗ"] = 4
            continue
        
        df.at[idx, "Требуемый УЗ"] = 4
    
    return df

### Главная функция
def run_scanning(path: str, update_callback = None)->pd.DataFrame:

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
    print(df)

    # --- Шаг 2 ---
    extracted_df = parsing(df, update_callback=update_callback) 
    time_step2 = time.time()
    print(f"Время парсинга информации: {round(time_step2 - time_step1, 2)} сек.")
    print(extracted_df)

    # --- Шаг 3 ---
    found_danger_df = seek_danger(extracted_df) 
    time_step3 = time.time()
    print(f"Время анализа по №152-ФЗ: {round(time_step3 - time_step2, 2)} сек.")
    
    # --- Шаг 4 ---
    categorized_df = categories(found_danger_df)
    time_step4 = time.time()
    print(f"Время анализа по №152-ФЗ: {round(time_step4 - time_step3, 2)} сек.")

    # --- Итог ---
    evaluated_df = evaluate_violations(categorized_df)

    time_step5 = time.time() - start_time
    print(f"\nВремя оценки нарушений: {round(time_step4, 2)} сек.")

    evaluated_df.drop(columns=["Содержание"], inplace=True)
    evaluated_df.reset_index(drop=True, inplace=True)

    try:
        conn = sqlite3.connect("DataBase.db")
        evaluated_df.to_sql("scan_results", con = conn, if_exists = "replace")
        print("Успешно создана база данных")
        print(evaluated_df.columns)

    except Exception as e:

        print(f"Создать базу данных не удалось: {e}")

    finally:

        conn.close()

   
    print(evaluated_df["Категории"])
    # Итог
    total_time = time.time() - start_time
    print(f"\nОБЩЕЕ ВРЕМЯ РАБОТЫ: {round(total_time, 2)} сек.")
    
    return evaluated_df