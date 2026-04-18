from pydantic import BaseModel
from typing import List, Optional

"""
Данные классы задают структуру того, как данные будут представлены
пользователю. Хоть явно у них и нет методов, однако, есть методы,
унаследованные у BaseModel. Перечислю:
.model_dump() - превращает объекст класса ScanResultSchema в словарь
.model_json_schema() - генерирует JSON-схему 
.model_validate(obj) - принимает на вход данные (SQL, Pandas и т.д.) 
и выполняет проверку, все ли с ними в порядке. Например, ошибка в типе
данных в колонке
.from_attributes(obj) - создает объект схемы напрямую из базы данных или
объекта

По сути они формируют структуру данных
"""

from pydantic import BaseModel, Field
from typing import Optional

class ScanResultSchema(BaseModel):
    """
    Данный класс показывает шаблон того, как должен выглядеть результат
    работы сканера
    """
    # Field помогает добавить описание (description) и пример (examples)
    # alias связывает переменную Python с именем колонки в БД/Pandas

    Имя_файла: str = Field(alias="Имя файла")
    Путь: str = Field()
    Расширение: str = Field()
    Дата_создания: str = Field(alias="Дата создания")
    Содержание: str = Field()
    Рейтинг_опасности: float = Field(alias="Рейтинг опасности")
    Найденные_ПДн: str = Field(alias="Найденные ПДн")
    Категории: str

    class Config:
        from_attributes = True
        populate_by_name = True

class ScanStatus(BaseModel):
    """
    Данный класс показывает шаблон того, как должен выглядеть результат
    опроса пользователем состояния запроса
    """
    task_id: str = Field(description="Уникальный идентификатор задачи")
    status: str = Field(description="Текущее состояние сканирования")
    message: Optional[str] = Field(None, description="Дополнительное инфо от сервера")
    current_file: str
    current_file_pos: int
    total_files: int

class PullQuite(BaseModel):
    """
    Данный класс показывает шаблон того, как должен выглядеть результат
    работы вытяжки основной информации из анализа
    """
    Просканированно: int = Field(description="Просканированно файлов")
    Самый_опасный_файл: str = Field(description="Самый опасный файл")
    Высшая_степень_опасности: float = Field(description="Наивысшая опасность")
    Детали: str = Field(description="Детали")

