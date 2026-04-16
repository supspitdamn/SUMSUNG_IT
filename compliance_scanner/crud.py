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
                        [Рейтинг опасности] DESC
                """
        df = pd.read_sql(queue, con = conn)
        return df.to_dict(orient="records")
    
    except Exception as e:

        print("Ошибка чтения БД: {e}")
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
