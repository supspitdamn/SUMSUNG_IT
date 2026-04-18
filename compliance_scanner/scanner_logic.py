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
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=8) as executor:
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
    
    АЛГОРИТМ РАБОТЫ:
    1. Инициализация справочника БИК
    2. Определение словарей ключевых слов для "мягких" категорий (ФИО, Адрес, Национальность и т.д.)
    3. Определение паттернов для "жестких" ПДн (СНИЛС, ИНН, телефон и т.д.)
    4. Для каждой строки датафрейма:
       a. Поиск "мягких" категорий: подсчёт количества вхождений ключевых слов
       b. Для категории "Адрес" дополнительно поиск по паттернам адресов
       c. Поиск "жестких" ПДн по паттернам с валидацией
       d. Формирование строки результата: ПДн1(кол-во),ПДн2(кол-во),...
    """
    import re
    from collections import defaultdict
    from datetime import datetime
    
    _init_bic_directory()
    
    # ========================================================================
    # 1. КЛЮЧЕВЫЕ СЛОВА ДЛЯ "МЯГКИХ" КАТЕГОРИЙ (только маркеры, не сами данные)
    #    Каждое вхождение ключевого слова увеличивает счётчик на 1
    # ========================================================================
    
     # 1.1 Дата рождения
    BIRTH_KEYWORDS = [
        "дат[ауеы]?\\s+рождени[яюйем]?", "день\\s+рождени[яюйем]?",
        "год\\s+рождени[яюйем]?", "г\\.?\\s*р\\.?", "г[ /-]р",
        "родил(?:ся|ась|ось|ись)", "рождени[яеюйем]?", "рожд[её]н[аы]?",
        "урожене[цк]", "место\\s+рожд",
        "birth\\s*(?:day|date)?", "dob", "d\\.?o\\.?b\\.?", "born",
        "geburt", "geboren",
    ]
    
    # 1.2 ФИО
    FIO_KEYWORDS = [
        "фамили[яюейи]", "имя", "отчеств[ао]?", "фио", "ф\\.?и\\.?о\\.?",
        "инициалы", "ф\\.?и\\.?",
        "full\\s*name", "surname", "first\\s*name", "last\\s*name",
        "middle\\s*name", "given\\s*name", "family\\s*name", "patronymic",
        "name:?\\s", "nombre", "apellido", "prénom", "nome", "cognome",
        "nachname", "vorname",
    ]
    
    # 1.3 Адрес (объединённая категория)
    ADDRESS_KEYWORDS = [
        "адрес", "регистраци[яи]", "зарегистрирован[аыо]?",
        "прописк[аеи]", "прописан[аыо]?", "прожива(?:ет|ю|ют|ние)",
        "местожительство", "жительство", "почтовый\\s+адрес",
        "индекс", "почтовый\\s+индекс",
        "место\\s+рожд", "урожене[цк]", "м\\.?р\\.?",
        "address", "residence", "registered", "place\\s+of\\s+(?:birth|residence|living)",
        "birthplace", "pob", "p\\.?o\\.?b\\.?", "postal\\s+address", "zip\\s*code",
        "adresse", "dirección", "indirizzo", "anschrift", "wohnort",
    ]
    
    # 1.4 Национальность
    NATIONALITY_KEYWORDS = [
        "национальност", "наци[яи]", "этнос", "этническ",
        "народност", "гражданств[ао]", "граждан(?:ин|ка)",
        "nationality", "nation", "ethnic(?:ity|\\s+group)?", "ethnos",
        "citizenship", "citizen", "nationalit[äé]",
    ]
    
    # 1.5 Раса
    RACE_KEYWORDS = [
        "рас[аы]", "расов",
        "европеоид", "кавказоид", "монголоид", "негроид", "австралоид", "американоид",
        "race", "racial", "caucas(?:ian|oid)", "mongoloid", "negroid", "australoid",
        "white\\s*(?:race|people)?", "black\\s*(?:race|people)?", "asian", "hispanic", "latino",
        "african", "european", "indigenous", "rasse", "razza", "raza",
    ]
    
    # 1.6 Религиозные убеждения
    RELIGION_KEYWORDS = [
        "религи[яи]", "религиозн", "вероисповедани[ея]",
        "вер[аы]", "верующ", "конфесси[яи]",
        "православ", "христиан", "католи[кц]", "протестан",
        "ислам", "мусульман", "муслим", "будди[зс]", "иудаизм", "иудей",
        "индуизм", "атеи[зс]", "агностик", "сект[аы]", "культ",
        "religion", "religious", "faith", "creed", "denomination",
        "christian", "catholic", "protestant", "orthodox", "islam", "muslim",
        "buddhism", "judaism", "jewish", "hinduism", "atheism",
        "glaube", "glaubens", "religi[ös]",
    ]
    
    # 1.7 Политические убеждения
    POLITICAL_KEYWORDS = [
        "политическ", "парти[яй]", "партийн",
        "коммунист", "либерал", "консерватор", "социал[ -]?демократ",
        "социалист", "националист", "анархист", "демократ", "монархист",
        "республикан", "оппозици[яи]", "электорат",
        "political", "politics", "party\\s+member", "communist", "liberal",
        "conservative", "social\\s*democrat", "socialist", "nationalist",
        "anarchist", "democrat", "monarchist", "republican", "opposition",
        "right[ -]wing", "left[ -]wing",
    ]
    
    # 1.8 Судимость
    CRIMINAL_KEYWORDS = [
        "судимост", "судим[аы]?", "не\\s*судим", "осужд[её]н[аы]?",
        "привлекал(?:ся|ась|ись)", "уголовн", "преступлен", "преступни[кц]",
        "правонарушен", "судебн", "приговор[её]н", "отбывал[аи]?",
        "заключени[ея]", "заключ[её]нн", "тюрьм", "лишени[ея]\\s+свободы",
        "условн", "погашен", "криминальн", "следстви[ея]", "дознани[ея]",
        "обвинени[ея]", "обвиняем", "подозреваем", "арестован[аы]?",
        "рецидив", "criminal\\s+(?:record|history|background)",
        "convict(?:ion|ed)", "crime", "offen[cs]e", "prosecut",
        "arrest(?:ed)?", "detention", "prison", "jail", "incarcerat",
        "sentenc(?:e|ed)", "probation", "felony", "misdemeanor",
        "vorstrafe", "vorbestraft", "casier\\s+judiciaire",
    ]
    
    # 1.9 Медицина
    MEDICAL_KEYWORDS = [
        "диагноз", "заболевани[еяй]", "болезн[ьи]", "анамнез", "жалоб[аы]?",
        "лечени[еяю]", "терапи[яи]", "мкб[ -]?1[01]", "icd[ -]?1[01]",
        "рецепт", "назначен[аоы]?", "таблетк[аи]", "дозировк[аи]", "доз[аы]",
        "больниц[аы]", "поликлиник[аи]", "медцентр", "клиник[аи]",
        "вра[чч]", "медицинск", "медкарт", "истори[яи]\\s+болезни",
        "пациент", "симптом[ы]?", "осмотр", "обследовани[ея]",
        "анализ[ы]?", "рентген", "мрт", "кт\\s", "узи", "экг",
        "хирурги[яч]", "операци[яи]", "стационар", "амбулаторн",
        "инвалидност", "инвалид", "хроническ", "аллерги[яч]",
        "онкологи[яч]", "рак[ао]?", "диабет", "гипертони[яч]", "астм[аы]",
        "эпилепси[яч]", "психиатри[яч]", "психологи[яч]", "депресси",
        "diagnos(?:is|e|ed)", "disease", "illness", "sickness", "condition",
        "medical\\s+(?:history|record)", "treatment", "therapy", "medic(?:ation|ine)",
        "prescription", "dosage", "symptom", "patient", "doctor", "physician",
        "hospital", "clinic", "examination", "checkup", "surgery", "disability",
        "chronic", "allergy", "oncology", "cancer", "diabetes", "hypertension",
        "asthma", "epilepsy", "psychiatry", "psychology", "depression",
    ]
    
    # ========================================================================
    # 2. ПАТТЕРНЫ ДЛЯ ПОИСКА "ЖЕСТКИХ" ПДн (СНИЛС, ИНН, телефон и т.д.)
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
        "Полис ОМС": r"\b(?:полис|омс|страховой\s+полис|медицинский\s+полис)\b\s*:?\s*\b\d{16}\b",
        "Биометрия: лицо": r"\[БИОМЕТРИЯ[^\]]*лицо",
        "Биометрия: глаза": r"\[БИОМЕТРИЯ[^\]]*глаза",
        "Биометрия: силуэт": r"\[БИОМЕТРИЯ[^\]]*силуэт",
        "Биометрия: подпись": r"\[БИОМЕТРИЯ[^\]]*подпись",
        "Биометрия: отпечаток": r"\[БИОМЕТРИЯ[^\]]*отпечаток",
        "Биометрия: голос": r"\[БИОМЕТРИЯ[^\]]*голос",
    }
    
    # ========================================================================
    # 3. ПАТТЕРНЫ ДЛЯ ПОИСКА АДРЕСОВ (дополнительно к ключевым словам)
    #    Используются типичные шаблоны российских и международных адресов
    # ========================================================================
    ADDRESS_PATTERNS_COMPILED = [
        re.compile(p, re.IGNORECASE) for p in [
            r'\b\d{6}\s*,?\s*(?:г\.?|город)\s*[А-Яа-я\s\-]+',
            r'(?:ул\.?|улица|пр-т|проспект|пер\.?|переулок|пл\.?|площадь|б-р|бульвар|наб\.?|набережная|ш\.?|шоссе)\s+[А-Яа-я0-9\s\-\.,]+\s*(?:д\.?|дом)\s*\d+[а-яА-Я]?(?:\s*(?:к\.?|корп\.?|корпус)\s*\d+)?(?:\s*(?:кв\.?|квартира)\s*\d+)?',
            r'\b\d{6}\b',
            r'(?:г\.?|город)\s*[А-Яа-я\s\-]+,\s*(?:ул\.?|улица|пр-т|проспект)\s*[А-Яа-я\s\-\.]+,\s*\d+',
            r'\b(?:область|край|республика|АО|автономный округ)\s+[А-Яа-я\s\-]+\b',
            r'\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Way|Court|Ct|Plaza|Pl)\.?\s*,?\s*[A-Za-z\s]+,\s*[A-Z]{2}\s*\d{5}',
            r'\b\d{5}(?:-\d{4})?\s+[A-Za-z\s]+\b',
            r'P\.?O\.?\s*Box\s*\d+',
            r'(?:адрес|address|addr\.?)\s*:?\s*[^\n\.]+',
        ]
    ]
    
    # ========================================================================
    # 4. КАРТА "МЯГКИХ" КАТЕГОРИЙ И ИХ КЛЮЧЕВЫХ СЛОВ
    # ========================================================================
    SOFT_CATEGORIES_COMPILED = {}
    
    for category_name, keywords in [
        ("Дата рождения", BIRTH_KEYWORDS),
        ("ФИО", FIO_KEYWORDS),
        ("Адрес", ADDRESS_KEYWORDS),
        ("Национальность", NATIONALITY_KEYWORDS),
        ("Раса", RACE_KEYWORDS),
        ("Религиозные убеждения", RELIGION_KEYWORDS),
        ("Политические убеждения", POLITICAL_KEYWORDS),
        ("Судимость", CRIMINAL_KEYWORDS),
        ("Медицина", MEDICAL_KEYWORDS),
    ]:
        # Сортируем по длине (длинные первыми, чтобы избежать частичных совпадений)
        sorted_keywords = sorted(keywords, key=len, reverse=True)
        # Создаём единый паттерн: (слово1|слово2|слово3)
        pattern = r'(?:' + '|'.join(re.escape(kw) for kw in sorted_keywords) + r')'
        # Компилируем с флагом IGNORECASE
        SOFT_CATEGORIES_COMPILED[category_name] = re.compile(pattern, re.IGNORECASE)
    
    # ========================================================================
    # 5. ФУНКЦИЯ ПОДСЧЁТА КЛЮЧЕВЫХ СЛОВ В ТЕКСТЕ
    #    Возвращает количество уникальных позиций вхождений
    # ========================================================================
    def count_soft_matches_fast(text: str, compiled_pattern) -> int:
        """
        Быстрый подсчёт количества вхождений ключевых слов.
        Использует прекомпилированный паттерн.
        """
        if not text or text == "НЕТ ДОСТУПА":
            return 0
        # finditer возвращает итератор, len(list()) считает количество
        return len(list(compiled_pattern.finditer(text)))
    
    # ========================================================================
    # 6. ФУНКЦИЯ ПОДСЧЁТА АДРЕСОВ ПО ПАТТЕРНАМ
    #    Ищет паттерны адресов и возвращает количество найденных
    # ========================================================================
    def count_address_patterns_fast(text: str) -> int:
        """Быстрый поиск адресов по прекомпилированным паттернам."""
        if not text or text == "НЕТ ДОСТУПА":
            return 0
        found_positions = set()
        for pattern in ADDRESS_PATTERNS_COMPILED:
            for match in pattern.finditer(text):
                found_positions.add(match.start())
        return len(found_positions)
    
    # ========================================================================
    # 7. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ВАЛИДАЦИИ ДЛЯ "ЖЕСТКИХ" ПДн
    # ========================================================================
    
    # Списки для валидации телефона
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
    
    def is_valid_snils(snils_str: str) -> bool:
        """Проверка контрольной суммы СНИЛС"""
        digits = re.sub(r'\D', '', snils_str)
        if len(digits) != 11 or digits == "00000000000":
            return False
        total = sum(int(digits[i]) * (9 - i) for i in range(9))
        check = total % 101
        if check == 100:
            check = 0
        return check == int(digits[9:])
    
    def is_valid_inn(inn_str: str) -> bool:
        """Проверка контрольной суммы ИНН (10 или 12 цифр)"""
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
        """Проверка формата и кода оператора телефона"""
        digits = re.sub(r'\D', '', phone_str)
        if len(digits) != 11:
            return False
        if digits[0] not in ['7', '8']:
            return False
        operator_code = digits[1:4]
        return operator_code in VALID_OPERATOR_CODES
    
    def is_valid_driver_license(license_str: str) -> bool:
        """Проверка формата водительского удостоверения"""
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
        """Проверка формата MRZ (машиночитаемая зона паспорта)"""
        mrz_clean = mrz_str.replace(' ', '').replace('\n', '').replace('\r', '')
        if len(mrz_clean) not in [44, 88]:
            return False
        if not re.match(r'^[A-Z0-9<]+$', mrz_clean):
            return False
        return True
    
    def is_valid_card_number(card_str: str) -> bool:
        """Проверка номера банковской карты по алгоритму Луна"""
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
        """Проверка БИК по справочнику ЦБ"""
        if not bic_str.isdigit() or len(bic_str) != 9:
            return False
        if not bic_str.startswith('04'):
            return False
        return bic_str in _VALID_BICS
    
    def is_valid_bank_account(account_str: str, bic_str: str = None) -> tuple:
        """Проверка расчётного счёта по БИК"""
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
        """Проверка CVV/CVC кода (3 цифры)"""
        digits = re.sub(r'\D', '', cvv_str)
        return len(digits) == 3 and digits.isdigit()
    
    def is_valid_oms_policy(policy_str: str) -> bool:
        """Проверка полиса ОМС (16 цифр с контрольной суммой)"""
        digits = re.sub(r'\D', '', policy_str)
        if len(digits) != 16:
            return False
        weights = [1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3]
        total = sum(int(d) * w for d, w in zip(digits, weights))
        return total % 10 == 0
    
    # ========================================================================
    # 8. ОСНОВНОЙ ЦИКЛ ОБРАБОТКИ ДАТАФРЕЙМА
    # ========================================================================
    for idx, row in df.iterrows():
        # Пропускаем строки с ошибками парсинга
        if str(row["Содержание"]).split(" ")[0] == "Ошибка":
            df.at[idx, "Найденные ПДн"] = "Нет никаких нарушений"
            continue
        
        info = str(row["Содержание"])
        info_lower = info.lower()
        pd_count = defaultdict(int)
        
        # ====================================================================
        # 8.1. ПОИСК "МЯГКИХ" КАТЕГОРИЙ ПО КЛЮЧЕВЫМ СЛОВАМ
        #      Каждое вхождение ключевого слова увеличивает счётчик на 1
        # ====================================================================
        for category_name, compiled_pattern in SOFT_CATEGORIES_COMPILED.items():
            count = count_soft_matches_fast(info, compiled_pattern)
            if count > 0:
                pd_count[category_name] += count
        
        # ====================================================================
        # 8.2. ДОПОЛНИТЕЛЬНЫЙ ПОИСК АДРЕСОВ ПО ПАТТЕРНАМ
        #      Ищем типичные шаблоны адресов и добавляем к счётчику
        # ====================================================================
        address_pattern_count = count_address_patterns_fast(info)
        if address_pattern_count > 0:
            pd_count["Адрес"] += address_pattern_count
        
        # ====================================================================
        # 8.3. ПОИСК "ЖЕСТКИХ" ПДн ПО ПАТТЕРНАМ С ВАЛИДАЦИЕЙ
        # ====================================================================
        
        # Предварительный поиск всех БИК в тексте (нужен для валидации счетов)
        found_bics = []
        for match in re.finditer(r'\b\d{9}\b', info):
            bic_candidate = match.group()
            if is_valid_bic(bic_candidate):
                found_bics.append({
                    'bic': bic_candidate,
                    'position': match.start(),
                    'end': match.end()
                })
        
        # Обработка каждого паттерна
        for pattern_name, pattern in patterns.items():
            # Для некоторых паттернов ищем в нижнем регистре
            text_to_search = info_lower if pattern_name in ["Паспорт", "CVV", "Полис ОМС"] else info
            
            for match in re.finditer(pattern, text_to_search, re.IGNORECASE):
                match_text = match.group()
                start_pos = match.start()
                
                valid = True
                
                # Валидация в зависимости от типа ПДн
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
        # 8.4. ФОРМИРОВАНИЕ РЕЗУЛЬТАТА
        #      Формат: ПДн1(кол-во),ПДн2(кол-во),...
        # ====================================================================
        if pd_count:
            result_parts = [f"{pd_type}({count})" for pd_type, count in pd_count.items()]
            df.at[idx, "Найденные ПДн"] = ",".join(result_parts)
        else:
            df.at[idx, "Найденные ПДн"] = "Нет никаких нарушений"
    print(df)
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
        "Адрес",  # Объединённая категория: адрес регистрации + место рождения + место проживания
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
    # 5. СПЕЦИАЛЬНЫЕ КАТЕГОРИИ ПДн (ст. 10 152-ФЗ)
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
            
            # Извлекаем тип ПДн и количество: "ФИО(3)" -> pd_type="ФИО", count=3
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