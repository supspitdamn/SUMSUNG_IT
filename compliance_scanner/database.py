import sqlite3
import os

DB_PATH = "DataBase.db"

def get_connection():

    """
    Функция получения соединения с БД. Реализация позволяет
    обращаться к элементам по имени, а не по индексу.
    """

    conn = sqlite3.connect(DB_PATH)

    conn.row_factory = sqlite3.Row #Позволит обращаться не по индексу, а по имени

    return conn

def init_db():

    """
    Функция создает таблицу в случае, если ее еще нет
    Выполняется при запуске программы
    """

    conn = get_connection()
    cursor = conn.cursor()

    queue = """
                CREATE TABLE IF NOT EXISTS scan_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    "Имя файла" TEXT,
                    "Путь" TEXT,
                    "Расширение" TEXT,
                    "Дата создания" TEXT,
                    "Содержание" TEXT,
                    "Рейтинг опасности" REAL,
                    "Найденные ПДн" TEXT
                )
            """
    cursor.execute(queue)
    conn.commit()
    cursor.close()
    print("База данных инициализирована")