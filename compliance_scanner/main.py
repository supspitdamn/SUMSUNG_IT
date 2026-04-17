import uuid
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, BackgroundTasks, HTTPException
from typing import List
# Импортируем наши кирпичики
from compliance_scanner import crud, schemas, database
from compliance_scanner.scanner_logic import run_scanning

# для запуска и тестирования в консоли пишем: pyhton -m uvicorn compliance_scanner.main:app --reload

tasks = {}

###

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Функция запускается при начале работы программы
    и формирует базу данных, если таковой еще нет,
    а также информирует пользователя о старте и конце
    работы программы
    """
    print(f"Сервер запущен")
    crud.clear_db()
    database.init_db()
    yield # приостанавливает выполнение функции до след. вызова
    print(f"Сервер отключен")

app = FastAPI(
    title="Сканнер на предмет нарушений №152-ФЗ",
      lifespan=lifespan)

@app.get("/")
async def root():
    return {"message": "Сервер сканирования ПДн запущен. Перейдите на /docs для работы."}


@app.post("/scan", response_model=schemas.ScanStatus)
async def start_scan(path: str, background_tasks: BackgroundTasks):
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Такого пути нет")
    
    crud.clear_db()
    
    task_id = str(uuid.uuid4())
    tasks[task_id] = {"status": "В процессе..."}

    background_tasks.add_task(perform_analysis, task_id, path)

    # Добавляем поле status, чтобы FastAPI не ругался
    return {
        "task_id": task_id, 
        "status": "В процессе...", 
        "message": "Начало сканирования"
    }

###

def perform_analysis(task_id: str, path: str) -> None:
    """
    Функция принимает на вход ID задания и путь для сканирования.
    Запускается импортированная функция из scanner_logic.py для
    сканирования. По исходу формируется .json структура (построчно df)
    """
    try:

        result_df = run_scanning(path)
        tasks[task_id] = {
            "Статус": "выполнено",
            "Результаты": result_df.to_dict(orient="records")
        }
    except Exception as e:
        tasks[task_id] = {"Статус": "ошибка", "Детали": str(e)}

###

@app.get("/result/{task_id}")
async def get_results(task_id: str) -> dict:
    """
    Функция по айдишнику возвращает пользователю ответ на его запрос
    """
    return tasks.get(task_id, {"error": "Task not found"})

###

@app.get("/db_results", response_model=List[schemas.ScanResultSchema])
async def get_all_from_db():
    """
    Эта функция используется для работы с жестким диском
    и БД на SQL после работы функции perform_analysis из main.py.
    Возвращает .json структуру отсортированную по убыванию колонки
    "Рейтинг опасности" 
    """
    info = crud.get_all_results()
    return info

@app.get("/db_quite_pull", response_model=List[schemas.PullQuite])
async def get_pull_quite_from_db():
    info = crud.get_pull_quite()
    return info