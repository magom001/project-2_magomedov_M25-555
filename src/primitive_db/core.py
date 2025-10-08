from typing import Dict, List

from .models import VALID_TYPES, Column, Table
from .utils import load_metadata, save_metadata


class DatabaseError(Exception):
    """Базовое исключение для операций базы данных."""
    pass


class TableExistsError(DatabaseError):
    """Исключение при попытке создать таблицу, которая уже существует."""
    pass


class TableNotFoundError(DatabaseError):
    """Исключение при попытке доступа к несуществующей таблице."""
    pass


class Database:
    """Основной класс базы данных для управления таблицами."""
    
    def __init__(self, metadata_file: str = "db_meta.json"):
        """
        Инициализировать базу данных.
        
        Args:
            metadata_file: Путь к JSON файлу для хранения метаданных таблиц
        """
        self.metadata_file = metadata_file
        self.tables: Dict[str, Table] = {}
        self.load_tables()
    
    def load_tables(self) -> None:
        """Загрузить таблицы из файла метаданных."""
        metadata = load_metadata(self.metadata_file)
        self.tables = {}
        
        for table_name, table_data in metadata.items():
            self.tables[table_name] = Table.from_dict(table_name, table_data)
    
    def save_tables(self) -> None:
        """Сохранить таблицы в файл метаданных."""
        metadata = {}
        for table_name, table in self.tables.items():
            metadata[table_name] = table.to_dict()
        
        save_metadata(self.metadata_file, metadata)
    
    def create_table(self, table_name: str, column_specs: List[str]) -> str:
        """
        Создать новую таблицу.
        
        Args:
            table_name: Имя создаваемой таблицы
            column_specs: Список спецификаций столбцов в формате "имя:тип"
            
        Returns:
            Сообщение об успехе
            
        Raises:
            TableExistsError: Если таблица уже существует
            ValueError: Если спецификация столбца недействительна
        """
        if table_name in self.tables:
            raise TableExistsError(f'Ошибка: Таблица "{table_name}" уже существует.')
        
        columns = []
        for spec in column_specs:
            if ":" not in spec:
                raise ValueError(f"Некорректное значение: {spec}. Попробуйте снова.")
            
            try:
                name, col_type = spec.split(":", 1)
                if not name or not col_type:
                    raise ValueError(
                        f"Некорректное значение: {spec}. Попробуйте снова."
                    )
                
                if col_type not in VALID_TYPES:
                    raise ValueError(
                        f"Некорректное значение: {col_type}. Попробуйте снова."
                    )
                
                columns.append(Column(name, col_type))
            except ValueError as e:
                if "Некорректное значение" in str(e):
                    raise e
                raise ValueError(f"Некорректное значение: {spec}. Попробуйте снова.")
        
        table = Table(table_name, columns)
        self.tables[table_name] = table
        
        self.save_tables()
        
        columns_str = ", ".join([f"{col.name}:{col.type}" for col in table.columns])

        return f'Таблица "{table_name}" успешно создана со столбцами: {columns_str}'
    
    def drop_table(self, table_name: str) -> str:
        """
        Удалить существующую таблицу.
        
        Args:
            table_name: Имя таблицы для удаления
            
        Returns:
            Сообщение об успехе
            
        Raises:
            TableNotFoundError: Если таблица не существует
        """
        if table_name not in self.tables:
            raise TableNotFoundError(f'Ошибка: Таблица "{table_name}" не существует.')
        
        del self.tables[table_name]
        
        self.save_tables()
        
        return f'Таблица "{table_name}" успешно удалена.'
    
    def list_tables(self) -> str:
        """
        Показать список всех существующих таблиц.
        
        Returns:
            Строка с именами таблиц или сообщение об отсутствии таблиц
        """
        if not self.tables:
            return "Нет созданных таблиц."
        
        table_names = sorted(self.tables.keys())
        return "\n".join([f"- {name}" for name in table_names])
    
    def get_table(self, table_name: str) -> Table:
        """
        Получить таблицу по имени.
        
        Args:
            table_name: Имя таблицы
            
        Returns:
            Экземпляр таблицы
            
        Raises:
            TableNotFoundError: Если таблица не существует
        """
        if table_name not in self.tables:
            raise TableNotFoundError(f'Таблица "{table_name}" не существует.')
        
        return self.tables[table_name]
    
    def table_exists(self, table_name: str) -> bool:
        """Проверить, существует ли таблица."""
        return table_name in self.tables
    
    def get_table_count(self) -> int:
        """Получить количество таблиц в базе данных."""
        return len(self.tables)
