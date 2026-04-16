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
    # Field помогает добавить описание (description) и пример (examples)
    # alias связывает переменную Python с именем колонки в БД/Pandas
    Имя_файла: str = Field(alias="Имя файла", description="Название файла без расширения", examples=["passport_scan"])
    Путь: str = Field(description="Полный путь к файлу")
    Расширение: str = Field(description="Тип файла", examples=[".pdf"])
    Дата_создания: str = Field(alias="Дата создания", description="Временная метка создания файла")
    Содержание: str = Field(description="Краткий результат парсинга содержимого")
    Рейтинг_опасности: float = Field(alias="Рейтинг опасности", description="Оценка нарушения 152-ФЗ", examples=[8.5])
    Найденные_ПДн: str = Field(alias="Найденные ПДн", description="Типы обнаруженных персональных данных")

    class Config:
        from_attributes = True
        populate_by_name = True

class ScanStatus(BaseModel):
    task_id: str = Field(description="Уникальный идентификатор задачи")
    status: str = Field(description="Текущее состояние сканирования")
    message: Optional[str] = Field(None, description="Дополнительное инфо от сервера")
