from dataclasses import dataclass
from typing import Any, Dict, List

from .constants import VALID_TYPES


@dataclass
class Column:
    """Представляет столбец базы данных."""
    
    name: str
    type: str
    
    def __post_init__(self):
        """Проверить тип столбца после инициализации."""
        if self.type not in VALID_TYPES:
            raise ValueError(f"Недопустимый тип столбца: {self.type}")
    
    def to_dict(self) -> Dict[str, str]:
        """Преобразовать столбец в словарное представление."""
        return {"name": self.name, "type": self.type}


class Table:
    """Представляет таблицу базы данных."""
    
    def __init__(self, name: str, columns: List[Column] = None):
        """
        Инициализировать таблицу.
        
        Args:
            name: Имя таблицы
            columns: Список столбцов (столбец ID будет добавлен автоматически)
        """
        self.name = name
        self.columns: List[Column] = []
        
        # Всегда добавляем столбец ID как первый столбец
        self.columns.append(Column("ID", "int"))
        
        # Добавляем пользовательские столбцы
        if columns:
            self.columns.extend(columns)
    
    def add_column(self, column: Column) -> None:
        """Добавить столбец в таблицу."""
        # Проверяем, существует ли столбец уже
        if any(col.name == column.name for col in self.columns):
            raise ValueError(f"Столбец '{column.name}' уже существует")
        
        self.columns.append(column)
    
    def get_column(self, name: str) -> Column:
        """Получить столбец по имени."""
        for column in self.columns:
            if column.name == name:
                return column
        raise ValueError(f"Столбец '{name}' не найден")
    
    def list_columns(self) -> List[str]:
        """Получить список имён столбцов."""
        return [col.name for col in self.columns]
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразовать таблицу в словарное представление для JSON сериализации."""
        return {
            "name": self.name,
            "columns": {col.name: col.type for col in self.columns}
        }
    
    @classmethod
    def from_dict(cls, name: str, data: Dict[str, Any]) -> "Table":
        """Создать таблицу из словарного представления."""
        table = cls.__new__(cls)  # Создать экземпляр без вызова __init__
        table.name = name
        table.columns = []
        
        # Преобразовать словарь столбцов обратно в объекты Column
        for col_name, col_type in data["columns"].items():
            table.columns.append(Column(col_name, col_type))
        
        return table
    
    def __str__(self) -> str:
        """Строковое представление таблицы."""
        columns_str = ", ".join([f"{col.name}:{col.type}" for col in self.columns])
        return f"Таблица '{self.name}' со столбцами: {columns_str}"
    
    def __repr__(self) -> str:
        """Отладочное представление таблицы."""
        return f"Table(name='{self.name}', columns={self.columns})"