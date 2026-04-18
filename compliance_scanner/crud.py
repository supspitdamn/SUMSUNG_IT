import pandas as pd
from compliance_scanner.database import get_connection

def save_scan_results(df: pd.DataFrame):
    """
    Функция выполняет сохранение df в базу данных SQL
    """

    conn = get_connection()
    try:

        df.to_sql("scan_results",
                   con=conn,
                     if_exists="append",
                       index = False)
        
        print("БД успешно сохранена")
    
    except Exception as e:
        print(f"Ошибка формирования БД: {e}")
    
    finally:
        conn.close()

def get_all_results() -> list:

    """
    Функция обращается к базе данных и вытягивает всю информацию
    из нее
    """

    conn = get_connection()

    try:
        queue = """
                    SELECT
                       *
                    FROM
                        scan_results
                    ORDER BY
                        [Требуемый УЗ] DESC
                """
        df = pd.read_sql(queue, con = conn)
        return df.to_dict(orient="records")
    
    except Exception as e:

        print(f"Ошибка чтения БД: {e}")
        return []
    
    finally:

        conn.close()

def get_pull_quite() ->list:

    """
    Функция принимает возвращает список выжимки из базы данных
    """

    conn = get_connection()

    try:

        queue = """
            SELECT 
                (SELECT COUNT(*) FROM scan_results) AS Просканированно,
                [Имя файла] AS Самый_опасный_файл,
                [Требуемый УЗ] AS Высшая_степень_опасности,
                [Найденные ПДн] AS Детали
            FROM scan_results
            ORDER BY [Требуемый УЗ] DESC
            LIMIT 1
        """

        df = pd.read_sql(queue, con=conn)
        return df.to_dict(orient="records")
    
    except Exception as e:
        print(f"При выгрузке выжимки произошла ошибка: {e}")
        return []
    finally:
        conn.close()

def clear_db():
    """
    Функция чистит базу данных
    """
    conn = get_connection()

    try:

        cursor = conn.cursor()
        cursor.execute("""DELETE FROM scan_results""")
        conn.commit()

    except Exception as e:

        print(f"Ошибка очистки базы данных: {e}")
    
    finally:
        
        conn.close()
