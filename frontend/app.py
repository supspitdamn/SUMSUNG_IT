import streamlit as st
import requests
import pandas as pd
import matplotlib.pyplot as plt
import time
import datetime
import io
import zipfile
import os
import sqlite3
import tempfile

st.set_page_config(page_title="152-ФЗ Сканер", layout="wide")
BASE_URL = "http://127.0.0.1:8000"

if 'scan_finished' not in st.session_state: 
    st.session_state['scan_finished'] = False 
if 'show_full_report' not in st.session_state:
    st.session_state['show_full_report'] = False
if "quite_res" not in st.session_state:
    st.session_state["quite_res"] = None
if "df_report" not in st.session_state:
    st.session_state["df_report"] = pd.DataFrame()
if "loaded_from_file" not in st.session_state:
    st.session_state["loaded_from_file"] = False


# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def generate_markdown_report(quite_res, df_report):
    now = datetime.datetime.now().strftime("%d.%m.%Y %H:%M")
    md = []
    md.append(f"# Отчет по результатам сканирования (152-ФЗ)")
    md.append(f"*Сгенерировано: {now}*\n")
    md.append("## 1. Краткая сводка")
    if quite_res:
        data = quite_res if not isinstance(quite_res, list) else quite_res[0]
        md.append(f"- **Просканировано:** {data.get('Просканированно', 0)} шт.")
        md.append(f"- **Максимальный рейтинг опасности:** {data.get('Высшая_степень_опасности', 0):.1f}")
        md.append(f"- **Самый опасный файл:** `{data.get('Самый_опасный_файл', 'Нет')}`")
    md.append("\n---\n")
    md.append("## 2. Аналитические графики")
    md.append("### Распределение уровней угроз")
    md.append("![Критические угрозы](BARCHART.png)\n")
    md.append("### Соотношение типов ПДн")
    md.append("![Соотношение типов ПДн](PIECHART.png)\n")
    md.append("\n---\n")
    md.append("## 3. Детальные результаты")
    if df_report is not None and not df_report.empty:
        temp_df = df_report.copy()
        md.append(temp_df.to_markdown(index=False))
    else:
        md.append("*Таблица данных пуста.*")
    return "\n".join(md)


def create_zip_archive(quite_res, df_report):
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        md_text = generate_markdown_report(quite_res, df_report)
        zf.writestr("152_fz_report.md", md_text)
        path_to_visual = "visual" 
        images = ["BARCHART.png", "PIECHART.png"]
        for img_name in images:
            img_path = os.path.join(path_to_visual, img_name)
            if os.path.exists(img_path):
                zf.write(img_path, img_name)
    zip_buffer.seek(0)
    return zip_buffer


def is_too_big(df: pd.DataFrame) -> bool:
    return df.memory_usage(deep=True).sum() / (1024**2) >= 200


def load_db_file(uploaded_bytes: bytes) -> pd.DataFrame:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp.write(uploaded_bytes)
        tmp_path = tmp.name
    try:
        conn = sqlite3.connect(tmp_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='scan_results'"
        )
        if cursor.fetchone() is None:
            raise ValueError(
                "В загруженном .db файле нет таблицы 'scan_results'. "
                "Убедитесь, что загружаете файл DataBase.db, сгенерированный сканером."
            )
        df = pd.read_sql("SELECT * FROM scan_results ORDER BY [Требуемый УЗ] DESC", conn)
        if "id" in df.columns:
            df = df.drop(columns=["id"])
        conn.close()
        return df
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def build_quite_res_from_df(df: pd.DataFrame) -> list:
    if df.empty:
        return []
    col_uz   = "Требуемый УЗ"
    col_file = "Имя файла"
    col_pdn  = "Найденные ПДн"
    max_idx = df[col_uz].idxmax()
    return [{
        "Просканированно":          len(df),
        "Самый_опасный_файл":       str(df.at[max_idx, col_file]),
        "Высшая_степень_опасности": float(df[col_uz].max()),
        "Детали":                   str(df.at[max_idx, col_pdn])
    }]


def generate_leaderboard_csv(df: pd.DataFrame) -> bytes:
    """
    Генерирует result.csv для лидерборда.
    Формат: size,time,name
    Включает ТОЛЬКО файлы, в которых найдены ПДн.
    """
    col_pdn  = "Найденные_ПДн" if "Найденные_ПДн" in df.columns else "Найденные ПДн"
    col_path = "Путь"

    # Фильтруем: оставляем только файлы с реальными ПДн
    mask = (
        df[col_pdn].notna() &
        (df[col_pdn] != "NO") &
        (df[col_pdn].str.strip() != "")
    )
    filtered = df[mask].copy()

    if filtered.empty:
        return b"size,time,name\n"

    rows = []
    for _, row in filtered.iterrows():
        file_path = row[col_path]

        # --- size: размер файла в байтах ---
        try:
            size = os.path.getsize(file_path)
        except (OSError, TypeError):
            # Файл недоступен (удалён, другая машина) — пропускаем
            continue

        # --- time: дата модификации в формате "mon dd HH:MM" (как ls -l) ---
        try:
            mtime = os.path.getmtime(file_path)
            dt = datetime.datetime.fromtimestamp(mtime)
            # Формат: "sep 26 18:31" — месяц строчными, как в примере
            time_str = dt.strftime("%b %d %H:%M").lower()
            # Убираем ведущий ноль у дня, если есть: "sep 06" -> "sep  6"
            # (стандартный ls формат — но в примере задания "sep 26", оставляем как есть)
        except (OSError, TypeError):
            continue

        # --- name: имя файла с расширением (ОРИГИНАЛЬНЫЙ регистр!) ---
        name = os.path.basename(file_path)

        rows.append({
            "size": size,
            "time": time_str,
            "name": name
        })

    result_df = pd.DataFrame(rows, columns=["size", "time", "name"])

    # Убираем возможные дубли по имени файла
    result_df = result_df.drop_duplicates(subset=["name"])

    # Убираем строки с пропусками
    result_df = result_df.dropna()

    buf = io.BytesIO()
    result_df.to_csv(buf, index=False, encoding="utf-8", lineterminator="\n")
    buf.seek(0)
    return buf.getvalue()


def render_report(df: pd.DataFrame, quite_res):
    st.subheader("Краткая сводка")
    if quite_res:
        data = quite_res[0] if isinstance(quite_res, list) else quite_res
        col1, col2, col3 = st.columns(3)
        col1.metric("Просканировано", f"{data.get('Просканированно', 0)} шт.")
        col2.metric("Макс. рейтинг", f"{data.get('Высшая_степень_опасности', 0):.1f}")
        danger_file = data.get('Самый_опасный_файл', 'Нет')
        short_file = str(danger_file).replace('\\', '/').split('/')[-1]
        col3.metric("Самый опасный", short_file)
        if data.get('Детали'):
            st.info(f"**Детали анализа:** {data.get('Детали')}")

    st.divider()
    st.subheader("Критические угрозы")
    if not df.empty:
        col_uz   = "Требуемый_УЗ" if "Требуемый_УЗ" in df.columns else "Требуемый УЗ"
        col_file = "Имя_файла"    if "Имя_файла"    in df.columns else "Имя файла"
        top_10 = df.sort_values(by=col_uz, ascending=True).tail(10)
        fig, ax = plt.subplots(figsize=(10, 6))
        display_names = top_10[col_file].apply(
            lambda x: str(x).replace('\\', '/').split('/')[-1]
        )
        bars = ax.barh(display_names, top_10[col_uz], color="r")
        ax.set_xlabel('Уровень опасности')
        ax.grid(axis='x', linestyle='--', alpha=0.7)
        for bar in bars:
            ax.text(bar.get_width() + 0.1,
                    bar.get_y() + bar.get_height() / 2,
                    f'{bar.get_width():.1f}', va='center')
        st.pyplot(fig)
        os.makedirs("visual", exist_ok=True)
        fig.savefig("./visual/BARCHART.png", bbox_inches='tight')
        plt.close(fig)

    st.subheader("Соотношение типов ПДн")
    try:
        col_pdn = "Найденные_ПДн" if "Найденные_ПДн" in df.columns else "Найденные ПДн"
        expanded_types = df[col_pdn].str.split(',').explode().str.strip()
        expanded_types = expanded_types[
            (expanded_types != "") & (expanded_types != "NO") & (expanded_types.notna())
        ]
        type_counts = expanded_types.value_counts()
        if not type_counts.empty:
            fig2, ax2 = plt.subplots(figsize=(10, 6))
            total = type_counts.sum()
            legend_labels = [
                f'{label}: {val} ({(val/total)*100:.1f}%)'
                for label, val in type_counts.items()
            ]
            wedges, texts = ax2.pie(
                type_counts, startangle=90,
                colors=plt.cm.Pastel1.colors,
                wedgeprops={'edgecolor': 'white'}
            )
            ax2.legend(wedges, legend_labels, title="Типы ПДн",
                       loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))
            ax2.axis('equal')
            fig2.savefig("./visual/PIECHART.png", bbox_inches='tight', dpi=300)
            st.pyplot(fig2)
            plt.close(fig2)
        else:
            st.info("Нет данных для круговой диаграммы.")
    except Exception as e:
        st.info(f"Круговая диаграмма не построена. Детали: {e}")


# ============================================================
# ЗАГОЛОВОК
# ============================================================
st.title("Система анализа базы данных на предмет нарушений №152-ФЗ")
st.markdown("---")


# ============================================================
# САЙДБАР
# ============================================================
st.sidebar.header("Настройки")
path_to_scan = st.sidebar.text_input("Путь к папке:", placeholder="C:/Data")
start_button = st.sidebar.button("Запустить анализ", type="primary")

st.sidebar.markdown("---")

# ---------- ЗАГРУЗКА ГОТОВОЙ .db ----------
st.sidebar.subheader("Загрузить готовую БД")
uploaded_db = st.sidebar.file_uploader(
    "Файл DataBase.db",
    type=["db"],
    help="Загрузите ранее сформированный DataBase.db, чтобы построить отчёт без повторного сканирования."
)

if uploaded_db is not None:
    try:
        loaded_df = load_db_file(uploaded_db.getvalue())
        if loaded_df.empty:
            st.sidebar.warning("Таблица scan_results в загруженном файле пуста.")
        else:
            st.session_state["df_report"]       = loaded_df
            st.session_state["quite_res"]        = build_quite_res_from_df(loaded_df)
            st.session_state["scan_finished"]    = True
            st.session_state["loaded_from_file"] = True
            st.sidebar.success(f"Загружено {len(loaded_df)} записей из БД")
    except ValueError as ve:
        st.sidebar.error(str(ve))
    except Exception as e:
        st.sidebar.error(f"Ошибка чтения .db файла: {e}")

st.sidebar.markdown("---")

if st.sidebar.button(label="Получить подробный отчет"):
    st.session_state['show_full_report'] = not st.session_state['show_full_report']


# ============================================================
# ЗАПУСК СКАНИРОВАНИЯ
# ============================================================
if start_button:
    st.session_state['scan_finished']    = False
    st.session_state['show_full_report'] = False
    st.session_state['loaded_from_file'] = False

    if path_to_scan:
        try:
            res = requests.post(f"{BASE_URL}/scan", params={"path": path_to_scan})
            if res.status_code == 200:
                task_id = res.json().get("task_id")
                file_logger  = st.empty()
                progress_bar = st.empty()

                with st.status("Идет анализ...", expanded=True) as status:
                    while True:
                        check = requests.get(f"{BASE_URL}/result/{task_id}").json()
                        current_file = check.get("current_file", "Подготовка...")
                        pos   = check.get("current_file_pos", 0)
                        total = check.get("total_files", 0)

                        if total > 0:
                            file_logger.write(
                                f"📁 **Обработка:** {current_file} ({pos} из {total})"
                            )
                            progress_bar.progress(pos / total)
                        else:
                            file_logger.write(f"📁 **Обработка:** {current_file}")

                        if check.get("status") == "выполнено":
                            file_logger.empty()
                            progress_bar.empty()
                            status.update(label="Готово!", state="complete", expanded=False)
                            st.session_state['scan_finished'] = True
                            break
                        if st.session_state["scan_finished"]:
                            break
                        time.sleep(0.5)
                st.rerun()

        except Exception as e:
            st.error(f"Ошибка связи: {e}")


# ============================================================
# ОТРИСОВКА РЕЗУЛЬТАТОВ
# ============================================================
if st.session_state['scan_finished']:

    if not st.session_state.get("loaded_from_file", False):
        try:
            response = requests.get(f"{BASE_URL}/db_quite_pull")
            st.session_state["quite_res"] = response.json()
            results = requests.get(f"{BASE_URL}/db_results").json()
            st.session_state["df_report"] = pd.DataFrame(results)
        except Exception as e:
            st.error(f"Ошибка получения данных от сервера: {e}")

    df        = st.session_state["df_report"]
    quite_res = st.session_state["quite_res"]

    if not df.empty:
        render_report(df, quite_res)
    else:
        st.warning("Нет данных для отображения.")


# ============================================================
# ПОДРОБНЫЙ ОТЧЁТ + СКАЧИВАНИЕ
# ============================================================
if st.session_state['show_full_report']:
    st.divider()
    st.title("Подробный отчет")

    try:
        df_report = st.session_state["df_report"]

        if not df_report.empty:
            if not is_too_big(df_report):
                st.dataframe(data=df_report, use_container_width=True)
            else:
                st.info("**Вес файла превышает 200 Мб. Доступен только в скачанном виде.**")

            buffer = io.BytesIO()
            df_report.to_csv(buffer, index=False, encoding='utf-8-sig')
            buffer.seek(0)

            st.download_button(
                label="Скачать .CSV",
                data=buffer,
                file_name='report_152_fz.csv',
                mime='text/csv',
            )

            st.download_button(
                label="Скачать .zip",
                data=create_zip_archive(
                    st.session_state["quite_res"],
                    st.session_state["df_report"]
                ),
                file_name="compliance_report.zip",
                mime="application/zip"
            )

            # ============================================================
            # КНОПКА ЛИДЕРБОРДА — result.csv
            # ============================================================
            st.divider()
            st.subheader("Экспорт для лидерборда")
            st.caption(
                "Генерирует `result.csv` в формате `size,time,name` — "
                "только файлы с найденными ПДн. Имена файлов сохраняют оригинальный регистр."
            )

            try:
                leaderboard_bytes = generate_leaderboard_csv(df_report)

                # Показываем превью
                preview_df = pd.read_csv(io.BytesIO(leaderboard_bytes))
                st.write(f"**Файлов с ПДн:** {len(preview_df)}")
                st.dataframe(preview_df.head(20), use_container_width=True)

                st.download_button(
                    label="Скачать result.csv",
                    data=leaderboard_bytes,
                    file_name="result.csv",
                    mime="text/csv",
                    type="primary"
                )
            except Exception as e:
                st.error(f"Ошибка генерации result.csv: {e}")

        else:
            st.info("Нет данных для отображения.")
    except Exception as e:
        st.error(f"Ошибка загрузки таблицы: {e}")
