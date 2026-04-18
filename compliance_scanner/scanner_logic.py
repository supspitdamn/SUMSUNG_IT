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


import mediapipe as mp

FFMPEG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ffmpeg.exe")
FFPROBE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ffprobe.exe")
pytesseract.pytesseract.tesseract_cmd = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "tesseract", "tesseract.exe"
)

VALID_BICS = set()
BIC_TO_BANK_INFO = {}

def load_bic_directory(xml_path: str = "20260417_ED807_full.xml") -> None:
    """
    Загружает справочник БИК из файла ED807.xml
    """
    global VALID_BICS, BIC_TO_BANK_INFO
    
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Пространство имён XML
        ns = {'ed': 'urn:cbr-ru:ed:v2.0'}
        
        for bic_entry in root.findall('.//ed:BICDirectoryEntry', ns):
            bic = bic_entry.get('BIC')
            if bic:
                VALID_BICS.add(bic)
                
                # Сохраняем информацию о банке
                participant = bic_entry.find('.//ed:ParticipantInfo', ns)
                if participant is not None:
                    bank_name = participant.get('NameP', '')
                    BIC_TO_BANK_INFO[bic] = {
                        'name': bank_name,
                        'region': participant.get('Rgn', ''),
                        'city': participant.get('Nnp', '')
                    }
        
        print(f"Загружено {len(VALID_BICS)} БИК из справочника")
    except Exception as e:
        print(f"Ошибка загрузки справочника БИК: {e}")


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

def parsing(df: pd.DataFrame,  update_callback = None) -> None:
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

            ".parquet" : "binary_engine"
        }
        return cases.get(extension, "skip")
    
    audio_model = whisper.load_model("base")

    size = len(df)

    for idx, row in df.iterrows():
        if update_callback:
            update_callback(str(row["Имя файла"] + str(row["Расширение"])), idx + 1, size)
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

        # Звуки
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

        # Бинарники
        elif engine == "binary_engine":
            try:

                temp = polars.read_parquet(path)
                
                raw_text = " ".join(temp.select(polars.all().cast(polars.Utf8)).to_series().to_list())
                
                df.at[idx, "Содержание"] = raw_text
            
            except Exception as e:
                error_msg = f"Ошибка в чтении бинарника. Детали: {e}"
                print(error_msg)
                df.at[idx, "Содержание"] = error_msg


    return df

def seek_danger(df: pd.DataFrame) -> pd.DataFrame:
    """
    функция seek_danger принимает на вход датафрейм с извлеченным текстом
    и возвращает датафрейм с измененной колонкой "найденные пдн".
    
    Формат записи: "ТипПДн(количество),ТипПДн2(количество2)"
    Пример: "ФИО(3),Телефон(2),Паспорт(1)"
    
    Канцеляризмы встроены непосредственно в паттерны поиска.
    Для специальных категорий используются списки допустимых значений.
    """
    
    import re
    import datetime
    from collections import defaultdict
    import xml.etree.ElementTree as ET
    
    # ========================================================================
    # 0. ЗАГРУЗКА СПРАВОЧНИКА БИК (при первом вызове)
    # ========================================================================
    if not hasattr(seek_danger, 'VALID_BICS'):
        seek_danger.VALID_BICS = set()
        seek_danger.BIC_TO_BANK_INFO = {}
        
        try:
            tree = ET.parse("20260417_ED807_full.xml")
            root = tree.getroot()
            ns = {'ed': 'urn:cbr-ru:ed:v2.0'}
            
            for bic_entry in root.findall('.//ed:BICDirectoryEntry', ns):
                bic = bic_entry.get('BIC')
                if bic:
                    seek_danger.VALID_BICS.add(bic)
                    
                    participant = bic_entry.find('.//ed:ParticipantInfo', ns)
                    if participant is not None:
                        seek_danger.BIC_TO_BANK_INFO[bic] = {
                            'name': participant.get('NameP', ''),
                            'region': participant.get('Rgn', ''),
                            'city': participant.get('Nnp', '')
                        }
            
            print(f"Загружено {len(seek_danger.VALID_BICS)} БИК из справочника")
        except Exception as e:
            print(f"Ошибка загрузки справочника БИК: {e}")
    
    # ========================================================================
    # 1. ПАТТЕРНЫ ДЛЯ ПОИСКА (канцеляризм + захват значения)
    # ========================================================================
    patterns = {
        # ===== КОНТАКТНЫЕ ДАННЫЕ (определяются по формату) =====
        "Телефон": r"(?:\+7|8)[\s\(-]?\d{3}[\s\)-]?\d{3}[\s-]?\d{2}[\s-]?\d{2}\b",
        "Email": r"[\w\.-]+@[\w\.-]+\.\w+",
        
        # ===== ГОСУДАРСТВЕННЫЕ ИДЕНТИФИКАТОРЫ =====
        "СНИЛС": r"\d{3}-\d{3}-\d{3}\s\d{2}\b",
        "ИНН": r"\b\d{10}\b|\b\d{12}\b",
        "Паспорт": r"\b(?:паспорт|серия|выдан|кем\s+выдан|паспортные\s+данные)\b\s*:?\s*\d{2}\s?\d{2}\s?\d{6}\b",
        "Водительское удостоверение": r"[АВЕКМНОРСТУХ]{2}\d{6}\b",
        "MRZ": r"[A-Z0-9<]{44,88}",
        
        # ===== БАНКОВСКИЕ РЕКВИЗИТЫ =====
        "БИК": r"\b\d{9}\b",
        "Банковский счет": r"\b\d{20}\b",
        
        # ===== ЛИЧНЫЕ ДАННЫЕ (канцеляризм + захват значения) =====
        # ФИО: только с канцеляризмами (фамилия, имя, отчество, фио, гражданин)
        "ФИО": r"\b(?:фамилия|имя|отчество|фио|ф\.и\.о\.|гражданин|гражданка)\b\s*:?\s*(?:[А-Я][а-я]+\s+[А-Я][а-я]+\s+[А-Я][а-я]+)",
        
        "Дата рождения": r"\b(?:дата\s+рожд(?:ения)?|день\s+рождения|год\s+рождения|родился|родилась|г\.р\.|рожд)\b\s*:?\s*\b(?:0[1-9]|[12][0-9]|3[01])[./-](?:0[1-9]|1[0-2])[./-](?:19|20)\d{2}\b",
        "Место рождения": r"\b(?:место\s+рожд(?:ения)?|родился|родилась|уроженец|уроженка|рожд)\b\s*:?\s*([А-Яа-я\s,\.]+)",
        "Адрес регистрации": r"\b(?:адрес|зарегистрирован|проживает|прописка|место\s+жительства|место\s+регистрации|регистрация)\b\s*:?\s*(?:г\.|город|ул\.?|улица|пр\.|проспект|пер\.|переулок|д\.|дом|кв\.|квартира|к\.?)\s*[А-Яа-я0-9\s,\.-]+",
        
        # ===== ПЛАТЕЖНАЯ ИНФОРМАЦИЯ =====
        "Банковская карта": r"\b(?:\d{4}[- ]?){3}\d{4}\b",
        "CVV": r"(?:cvv|cvc|код\s+безопасности|код\s+карты|cvv2/cvc2|код\s+cvv|cvv\s+код)\s*:?\s*\d{3}\b",
        
        # ===== ФИНАНСОВЫЕ ДАННЫЕ =====
        "Заработная плата": r"\b(?:зарплата|оклад|доход|зп|заработная\s+плата|ежемесячный\s+доход|среднемесячный\s+доход)\b\s*:?\s*\d+",
        
        # ===== МЕДИЦИНСКИЕ ДАННЫЕ =====
        "Медицина": r"\b(?:диагноз|заболевание|болезнь|анамнез|жалобы|лечение|терапия|мкб-\d+|рецепт|назначено|таблетки|дозировка|больница|поликлиника|медцентр|клиника|врач|медицинская\s+карта)\b",
        "Полис ОМС": r"\b(?:полис|омс|страховой\s+полис|медицинский\s+полис)\b\s*:?\s*\b\d{16}\b",
        
        # ===== СПЕЦИАЛЬНЫЕ КАТЕГОРИИ ПДн (канцеляризм + захват значения) =====
        "Национальность": r"\b(?:национальность|нация|этнос|национальная\s+принадлежность)\b\s*:?\s*([А-Яа-я]+)",
        "Раса": r"\b(?:раса|расовая\s+принадлежность)\b\s*:?\s*([А-Яа-я]+)",
        "Религиозные убеждения": r"\b(?:религия|вероисповедание|вера|религиозные\s+взгляды)\b\s*:?\s*([А-Яа-я]+)",
        "Политические убеждения": r"\b(?:партия|политические\s+убеждения|полит\s+взгляды|политическая\s+принадлежность)\b\s*:?\s*([А-Яа-я]+)",
        
        # Судимость: канцеляризмы + значение (есть/нет)
        "Судимость": r"\b(?:судимость|судим|осужден|привлекался|уголовное\s+дело|несудим)\b\s*:?\s*(?:есть|нет|отсутствует|имеется|не\s+имеется)",

        #Биометрия
        "Биометрия: лицо": r"\[БИОМЕТРИЯ[^\]]*лицо",
        "Биометрия: глаза": r"\[БИОМЕТРИЯ[^\]]*глаза",
        "Биометрия: силуэт": r"\[БИОМЕТРИЯ[^\]]*силуэт",
        "Биометрия: подпись": r"\[БИОМЕТРИЯ[^\]]*подпись",
        "Биометрия: отпечаток": r"\[БИОМЕТРИЯ[^\]]*отпечаток",
        "Биометрия: голос": r"\[БИОМЕТРИЯ[^\]]*голос",
    }
    
    # ========================================================================
    # 2. СПИСКИ ДЛЯ ПРОВЕРКИ ЗНАЧЕНИЙ
    # ========================================================================
    
    # Список рас
    RACE_VALUES = {
        "европеоид", "европеоидная", "европеоидной", "европеоидную", "европеоидный",
        "кавказоид", "кавказоидная", "кавказоидной", "кавказоидную", "кавказоидный",
        "монголоид", "монголоидная", "монголоидной", "монголоидную", "монголоидный",
        "негроид", "негроидная", "негроидной", "негроидную", "негроидный",
        "экваториальная", "экваториальной", "экваториальную",
        "австралоид", "австралоидная", "австралоидной", "австралоидную", "австралоидный",
        "американоид", "американоидная", "американоидной", "американоидную", "американоидный",
    }
    
    # Список национальностей РФ
    NATIONALITIES = {
        "русский", "русская", "татарин", "татарка", "украинец", "украинка",
        "башкир", "башкирка", "чуваш", "чувашка", "чеченец", "чеченка",
        "армянин", "армянка", "азербайджанец", "азербайджанка",
        "мордвин", "мордовка", "казах", "казашка", "белорус", "белоруска",
        "узбек", "узбечка", "таджик", "таджичка", "киргиз", "киргизка",
        "грузин", "грузинка", "молдаванин", "молдаванка", "немец", "немка",
        "еврей", "еврейка", "кореец", "кореянка", "китаец", "китаянка",
        "осетин", "осетинка", "якут", "якутка", "бурят", "бурятка",
        "ингуш", "ингушка", "лезгин", "лезгинка", "калмык", "калмычка",
        "аварец", "аварка", "даргинец", "даргинка", "кумык", "кумычка",
        "кабардинец", "кабардинка", "адыгеец", "адыгейка", "карачаевец", "карачаевка",
        "балкарец", "балкарка", "ногаец", "ногайка", "черкес", "черкешенка",
        "абхаз", "абхазка", "тувинец", "тувинка", "хакас", "хакаска",
        "алтаец", "алтайка", "мариец", "марийка", "удмурт", "удмуртка",
        "коми", "карел", "карелка", "финн", "финка", "эстонец", "эстонка",
        "латыш", "латышка", "литовец", "литовка", "поляк", "полька",
        "болгарин", "болгарка", "грек", "гречанка", "цыган", "цыганка",
        "вьетнамец", "вьетнамка",
    }
    
    # Список религиозных убеждений
    RELIGIONS = {
        "православие", "православия", "православию",
        "христианство", "христианства", "христианству", "христианин", "христианка",
        "ислам", "ислама", "исламу", "исламом", "мусульманин", "мусульманка",
        "буддизм", "буддизма", "буддизму", "буддизмом",
        "иудаизм", "иудаизма", "иудаизму", "иудаизмом",
        "католицизм", "католицизма", "католицизму", "католицизмом",
        "протестантизм", "протестантство",
        "индуизм", "индуизма", "индуизму", "индуизмом",
        "атеист", "атеистка", "атеизм",
        "агностик", "агностицизм"
    }
    
    # Список политических убеждений
    POLITICAL_VIEWS = {
        "коммунист", "коммунистические", "либерал", "либеральные",
        "консерватор", "консервативные", "социал-демократ", "социал-демократические",
        "националист", "националистические", "анархист", "анархические",
        "социалист", "социалистические", "демократ", "демократические",
        "монархист", "монархические", "фашист", "фашистские",
        "зеленые", "экологические", "центрист", "центристские",
        "аполитичный", "нейтральные", "не определился"
    }
    
    # ========================================================================
    # 3. ВАЛИДНЫЕ КОДЫ ОПЕРАТОРОВ РФ
    # ========================================================================
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
        return bic_str in seek_danger.VALID_BICS
    
    def is_valid_bank_account(account_str: str, bic_str: str = None) -> tuple:
        digits = re.sub(r'\D', '', account_str)
        if len(digits) != 20:
            return (False, "не 20 цифр")
        
        if bic_str and bic_str in seek_danger.VALID_BICS:
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
    
    def is_valid_date(date_str: str) -> tuple:
        date_formats = [
            r'(\d{2})\.(\d{2})\.(\d{4})',
            r'(\d{2})/(\d{2})/(\d{4})',
            r'(\d{2})-(\d{2})-(\d{4})',
        ]
        
        day, month, year = None, None, None
        parsed = False
        
        for fmt in date_formats:
            match = re.search(fmt, date_str)
            if match:
                day = int(match.group(1))
                month = int(match.group(2))
                year = int(match.group(3))
                parsed = True
                break
        
        if not parsed:
            match = re.search(r'(\d{2})(\d{2})(\d{4})', date_str)
            if match:
                day = int(match.group(1))
                month = int(match.group(2))
                year = int(match.group(3))
                parsed = True
        
        if not parsed:
            return (False, "неверный формат даты")
        
        try:
            datetime.date(year, month, day)
        except ValueError:
            return (False, "несуществующая дата")
        
        if year < 1900 or year > 2025:
            return (False, "год вне диапазона")
        
        return (True, "OK")
    
    def is_valid_cvv(cvv_str: str) -> bool:
        digits = re.sub(r'\D', '', cvv_str)
        return len(digits) == 3 and digits.isdigit()
    
    def is_valid_oms_policy(policy_str: str) -> bool:
        """
        Проверка полиса ОМС по контрольной сумме.
        Формат: 16 цифр.
        Контрольная сумма: сумма произведений цифр на веса (1,3,1,3,...) должна быть кратна 10.
        """
        digits = re.sub(r'\D', '', policy_str)
        
        if len(digits) != 16:
            return False
        
        weights = [1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3]
        
        total = 0
        for i, digit in enumerate(digits):
            total += int(digit) * weights[i]
        
        return total % 10 == 0
    
    def is_valid_race(race_str: str) -> bool:
        race_lower = race_str.lower().strip()
        return race_lower in RACE_VALUES
    
    def is_valid_nationality(nationality_str: str) -> bool:
        nationality_lower = nationality_str.lower().strip()
        return nationality_lower in NATIONALITIES
    
    def is_valid_religion(religion_str: str) -> bool:
        religion_lower = religion_str.lower().strip()
        return religion_lower in RELIGIONS
    
    def is_valid_political_view(view_str: str) -> bool:
        view_lower = view_str.lower().strip()
        return view_lower in POLITICAL_VIEWS
    
    def is_valid_birth_place(place_str: str) -> bool:
        return bool(place_str and len(place_str.strip()) >= 2)
    
    # ========================================================================
    # 5. ОСНОВНОЙ ЦИКЛ
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
        
        for pattern_name, pattern in patterns.items():
            # Для регистронезависимых паттернов используем нижний регистр
            if pattern_name in ["ФИО", "Адрес регистрации", "Заработная плата", "Медицина",
                                "Место рождения", "Национальность", "Раса", "Религиозные убеждения",
                                "Политические убеждения", "Судимость", "Дата рождения", "Паспорт",
                                "CVV", "Полис ОМС"]:
                text_to_search = info_lower
            else:
                text_to_search = info
            
            for match in re.finditer(pattern, text_to_search, re.IGNORECASE):
                match_text = match.group()
                start_pos = match.start()
                end_pos = match.end()
                
                valid = True
                
                # Валидация по типам
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
                elif pattern_name == "Дата рождения":
                    valid, _ = is_valid_date(match_text)
                elif pattern_name == "CVV":
                    valid = is_valid_cvv(match_text)
                elif pattern_name == "Полис ОМС":
                    valid = is_valid_oms_policy(match_text)
                elif pattern_name == "Раса":
                    race_match = re.search(r'([А-Яа-я]+)', match_text)
                    if race_match:
                        valid = is_valid_race(race_match.group(1))
                    else:
                        valid = False
                elif pattern_name == "Национальность":
                    nationality_match = re.search(r'([А-Яа-я]+)', match_text)
                    if nationality_match:
                        valid = is_valid_nationality(nationality_match.group(1))
                    else:
                        valid = False
                elif pattern_name == "Религиозные убеждения":
                    religion_match = re.search(r'([А-Яа-я]+)', match_text)
                    if religion_match:
                        valid = is_valid_religion(religion_match.group(1))
                    else:
                        valid = False
                elif pattern_name == "Политические убеждения":
                    view_match = re.search(r'([А-Яа-я]+)', match_text)
                    if view_match:
                        valid = is_valid_political_view(view_match.group(1))
                    else:
                        valid = False
                elif pattern_name == "Место рождения":
                    place_match = re.search(r'([А-Яа-я\s,\.]+)$', match_text)
                    if place_match:
                        valid = is_valid_birth_place(place_match.group(1))
                    else:
                        valid = False
                elif pattern_name in ["ФИО", "Паспорт", "Адрес регистрации",
                                      "Заработная плата", "Медицина", "Судимость"]:
                    valid = True
                
                if valid:
                    pd_count[pattern_name] += 1
                    break
        
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

    try:
        conn = sqlite3.connect("DataBase.db")
        evaluated_df.to_sql("scan_results", con = conn, if_exists = "replace")
        print("Успешно создана база данных")

    except Exception as e:

        print(f"Создать базу данных не удалось: {e}")

    finally:

        conn.close()

    # Итог
    total_time = time.time() - start_time
    print(f"\nОБЩЕЕ ВРЕМЯ РАБОТЫ: {round(total_time, 2)} сек.")
    
    return evaluated_df