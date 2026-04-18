import streamlit as st
import requests
import pandas as pd
import matplotlib.pyplot as plt
import time
import datetime

st.set_page_config(page_title="152-ФЗ Сканер", layout="wide")
BASE_URL = "http://127.0.0.1:8000" # Адрес сервера с АПИ

if 'scan_finished' not in st.session_state: 
    st.session_state['scan_finished'] = False 
if 'show_full_report' not in st.session_state:
    st.session_state['show_full_report'] = False
if "quite_res" not in st.session_state:
    st.session_state["quite_res"] = None
if "df_report" not in st.session_state:
    st.session_state["df_report"] = pd.DataFrame()

# Две данные переменные хранят информацию о состояниях кнопок. В стримлит файл app.py
# Каждый раз перезапускается системой при любом изменении

def generate_markdown_report(quite_res, df_report):

    now = datetime.datetime.now().strftime("%d.%m.%Y %H:%M")
    md = []

    # Заголовок
    md.append(f"# Отчет по результатам сканирования (152-ФЗ)")
    md.append(f"*Сгенерировано: {now}*\n")

    # 1. Краткая сводка
    md.append("## 1. Краткая сводка")
    if quite_res:
        data = quite_res if not isinstance(quite_res, list) else quite_res[0]
        md.append(f"- **Просканировано:** {data.get('Просканированно', 0)} шт.")
        md.append(f"- **Максимальный рейтинг опасности:** {data.get('Высшая_степень_опасности', 0):.1f}")
        md.append(f"- **Самый опасный файл:** `{data.get('Самый_опасный_файл', 'Нет')}`")
    
    md.append("\n---\n")

    # 2. Визуализация (Берем из папки visual)
    md.append("## 2. Аналитические графики")
    
    # Добавляем гистограмму
    md.append("### Распределение уровней угроз")
    md.append("![Критические угрозы](visual/BARCHART.png)\n")
    
    # Добавляем пайчарт
    md.append("### Соотношение типов ПДн")
    md.append("![Соотношение типов ПДн](visual/PIECHART.png)\n")

    md.append("\n---\n")

    # 3. Таблица результатов
    md.append("## 3. Детальные результаты")
    if df_report is not None and not df_report.empty:
        # Очистка текста для корректного отображения таблицы
        temp_df = df_report.copy()
        if "Содержание" in temp_df.columns:
            temp_df["Содержание"] = temp_df["Содержание"].str.replace(r'\s+', ' ', regex=True).str.slice(0, 150) + "..."
        
        md.append(temp_df.to_markdown(index=False))
    else:
        md.append("*Таблица данных пуста.*")

    return "\n".join(md)


st.title("Система анализа базы данных на предмет нарушений №152-ФЗ")
st.markdown("---")

st.sidebar.header("Настройки") # Заголовок левого окошка
path_to_scan = st.sidebar.text_input("Путь к папке:", placeholder="C:/Data")
start_button = st.sidebar.button("Запустить анализ", type="primary") # Кнопка, меняющая состояние

st.sidebar.markdown("---")
if st.sidebar.button(label="Получить подробный отчет"):
    st.session_state['show_full_report'] = not st.session_state['show_full_report'] # Простая инверсия состояния

if start_button: 
    st.session_state['scan_finished'] = False 
    st.session_state['show_full_report'] = False

    if path_to_scan: 
        try:
            res = requests.post(f"{BASE_URL}/scan", params={"path": path_to_scan}) 
            if res.status_code == 200: 
                task_id = res.json().get("task_id") 
                
                # Создаем контейнеры для лога и прогресс-бара
                file_logger = st.empty()
                progress_bar = st.empty() 

                with st.status("Идет анализ...", expanded=True) as status:
                    while True: 
                        check = requests.get(f"{BASE_URL}/result/{task_id}").json() 
                        
                        # Достаем данные для прогресса
                        current_file = check.get("current_file", "Подготовка...")
                        pos = check.get("current_file_pos", 0)
                        total = check.get("total_files", 0)

                        # Обновляем текстовый лог
                        if total > 0:
                            file_logger.write(f"📁 **Обработка:** {current_file} ({pos} из {total})")
                            print(pos)
                            print(total)
                            progress_bar.progress(pos / total)
                        else:
                            file_logger.write(f"📁 **Обработка:** {current_file}")

                        if check.get("status") == "выполнено": 
                            # Убираем временные элементы перед перезагрузкой
                            file_logger.empty()
                            progress_bar.empty()
                            
                            status.update(label="Готово!", state="complete", expanded=False)
                            st.session_state['scan_finished'] = True
                            break

                        if st.session_state["scan_finished"]:
                            break

                        time.sleep(0.5) # Достаточно одного sleep
                    
                st.rerun()

        except Exception as e:
            st.error(f"Ошибка связи: {e}")


if st.session_state['scan_finished']: # Сканирование завершено - выводим базовую информацию
    st.subheader("Краткая сводка")
    try:
        response = requests.get(f"{BASE_URL}/db_quite_pull") # снова отправляем запрос к АПИ - по короткой сводке
        quite_res = response.json() # переводим в джсончик
        st.session_state["quite_res"] = quite_res
        
        if quite_res: # Если возвращен объект не типа None, то идем дальше
            data = quite_res[0] if isinstance(quite_res, list) else quite_res # предостережение излишне
            # Берем первый элемент массива (который в теории является словарем) иначе просто сам результат - словарь
            col1, col2, col3 = st.columns(3) # формирование колонок
            col1.metric("Просканировано", f"{data.get('Просканированно', 0)} шт.") # Получаем ответ в формате QuitePull - подаставляем
            col2.metric("Макс. рейтинг", f"{data.get('Высшая_степень_опасности', 0):.1f}") # Получаем ответ в формате QuitePull - подаставляем
            
            danger_file = data.get('Самый_опасный_файл', 'Нет') # Получаем ответ в формате QuitePull - подаставляем
            short_file = str(danger_file).replace('\\', '/').split('/')[-1] 
            col3.metric("Самый опасный", short_file) # Получаем ответ в формате QuitePull - подаставляем

            if data.get('Детали'):
                st.info(f"**Детали анализа:** {data.get('Детали')}")
        
        st.divider() # Визуально отделяем от следующей части
        st.subheader("Критические угрозы") 
        
        results = requests.get(f"{BASE_URL}/db_results").json() # Получаем полную БД
        df = pd.DataFrame(results) # Представляем в виде pandas df
        st.session_state["df_report"] = df

        if not df.empty: 
            col_name = "Требуемый_УЗ" if "Требуемый_УЗ" in df.columns else "Требуемый УЗ"
            top_10 = df.sort_values(by=col_name, ascending=True).tail(10)
            
            fig, ax = plt.subplots(figsize=(10, 6))
            display_names = top_10['Имя файла'].apply(lambda x: str(x).replace('\\', '/').split('/')[-1])
            
            bars = ax.barh(display_names, top_10[col_name], color="r")
            ax.set_xlabel('Уровень опасности')
            ax.grid(axis='x', linestyle='--', alpha=0.7)
            
            for bar in bars:
                ax.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2, 
                        f'{bar.get_width():.1f}', va='center')
            st.pyplot(fig)
            plt.close(fig)
            fig.savefig("./visual/BARCHART.png", bbox_inches='tight')

    except Exception as e:
        st.error(f"Ошибка отображения данных: {e}")
    
    # ПАЙЧАРТ
    st.subheader("Соотношение типов ПДн")
    try:

        expanded_types = df['Найденные ПДн'].str.split(',').explode().str.strip()
        
        # Теперь считаем чистые уникальные типы
        type_counts = expanded_types.value_counts() 

        fig2, ax2 = plt.subplots(figsize=(8, 6))
        
        ax2.pie(
            type_counts, 
            labels=type_counts.index, 
            autopct='%1.1f%%', 
            startangle=90, 
            colors=plt.cm.Pastel1.colors,
            wedgeprops={'edgecolor': 'white'}
        )
        ax2.axis('equal') 

        st.pyplot(fig2)
        plt.close(fig2)
        fig2.savefig("./visual/PIECHART.png", bbox_inches='tight')
        
    except Exception as e:
        st.info(f"Круговая диаграмма не построена. Детали: {e}")



if st.session_state['show_full_report']: 
    st.divider()    
    st.title("Подробный отчет")
    try:
        df_report = st.session_state["df_report"]
        if not df_report.empty:

            st.dataframe(data=df_report, use_container_width=True) 

            csv_file = df_report.to_csv(index=False).encode('utf-8-sig')
            
            st.download_button(
                label="Скачать .CSV",
                data=csv_file,
                file_name='report_152_fz.csv',
                mime='text/csv',
            )

            st.download_button(
                label = "Скачать .md",
                data = generate_markdown_report(st.session_state["quite_res"], st.session_state["df_report"]),
                file_name = "report_152_fz.md",
                mime = "text/markdown"
            )

        else:
            st.info("Нет данных для отображения.")
    except Exception as e:
        st.error(f"Ошибка загрузки таблицы: {e}")
