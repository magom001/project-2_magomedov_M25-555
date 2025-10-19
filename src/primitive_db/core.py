from typing import Any, Dict, List, Optional

from .constants import META_FILE, VALID_TYPES
from .decorators import confirm_action, create_cacher, handle_db_errors, log_time
from .models import Column, Table
from .utils import (
    delete_table_data,
    load_metadata,
    load_table_data,
    save_metadata,
    save_table_data,
)


class DatabaseError(Exception):
    """Базовое исключение для операций базы данных."""
    pass


class TableExistsError(DatabaseError):
    """Исключение при попытке создать таблицу, которая уже существует."""
    pass


class TableNotFoundError(DatabaseError):
    """Исключение при попытке доступа к несуществующей таблице."""
    pass


class ValidationError(DatabaseError):
    """Исключение при ошибке валидации данных."""
    pass


class RecordNotFoundError(DatabaseError):
    """Исключение когда запись не найдена."""
    pass


class Database:
    """Основной класс базы данных для управления таблицами."""
    
    def __init__(self, metadata_file: str = META_FILE):
        """
        Инициализировать базу данных.
        
        Args:
            metadata_file: Путь к JSON файлу для хранения метаданных таблиц
        """
        self.metadata_file = metadata_file
        self.tables: Dict[str, Table] = {}
        self.load_tables()
        # Создаем функцию кеширования для select-запросов
        self._cache_select = create_cacher()
    
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
    
    def _clear_select_cache(self) -> None:
        """Очистить кеш select-запросов при изменении данных."""
        # Создаем новую функцию кеширования, чтобы очистить старый кеш
        self._cache_select = create_cacher()
    
    @handle_db_errors
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
    
    @handle_db_errors
    @confirm_action("удаление таблицы")
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
        
        # Remove table
        del self.tables[table_name]
        
        # Delete table data file
        delete_table_data(table_name)
        
        # Save to file
        self.save_tables()
        
        return f'Таблица "{table_name}" успешно удалена.'
    
    @handle_db_errors
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
    
    def _validate_value(self, value: str, expected_type: str) -> Any:
        """
        Валидировать и преобразовать значение в нужный тип.
        
        Args:
            value: Строковое значение для валидации
            expected_type: Ожидаемый тип ('int', 'str', 'bool')
            
        Returns:
            Преобразованное значение
            
        Raises:
            ValidationError: Если значение не соответствует типу
        """
        try:
            if expected_type == "int":
                return int(value)
            elif expected_type == "str":
                # Убираем кавычки если есть
                if value.startswith('"') and value.endswith('"'):
                    return value[1:-1]
                return value
            elif expected_type == "bool":
                if value.lower() in ("true", "1", "yes"):
                    return True
                elif value.lower() in ("false", "0", "no"):
                    return False
                else:
                    raise ValueError(f"Некорректное булево значение: {value}")
            else:
                raise ValidationError(f"Неизвестный тип: {expected_type}")
        except (ValueError, TypeError) as e:
            raise ValidationError(
                f"Не удалось преобразовать '{value}' в тип {expected_type}: {e}"
            )
    
    @handle_db_errors
    @log_time
    def insert(self, table_name: str, values: List[str]) -> str:
        """
        Вставить новую запись в таблицу.
        
        Args:
            table_name: Имя таблицы
            values: Список значений (без ID)
            
        Returns:
            Сообщение об успехе
            
        Raises:
            TableNotFoundError: Если таблица не существует
            ValidationError: Если данные некорректны
        """
        # Проверить существование таблицы
        table = self.get_table(table_name)
        
        # Проверить количество значений (без ID)
        expected_count = len(table.columns) - 1  # Минус ID
        if len(values) != expected_count:
            raise ValidationError(
                f"Ожидается {expected_count} значений, получено {len(values)}"
            )
        
        # Загрузить данные таблицы
        table_data = load_table_data(table_name)
        
        # Генерировать новый ID
        if table_data:
            max_id = max(record["ID"] for record in table_data)
            new_id = max_id + 1
        else:
            new_id = 1
        
        # Создать новую запись
        record = {"ID": new_id}
        
        # Валидировать и добавить значения
        for i, value in enumerate(values):
            column = table.columns[i + 1]  # +1 чтобы пропустить ID
            validated_value = self._validate_value(value, column.type)
            record[column.name] = validated_value
        
        # Добавить запись
        table_data.append(record)
        
        # Сохранить данные
        save_table_data(table_name, table_data)
        
        # Очистить кеш select-запросов
        self._clear_select_cache()
        
        return f'Запись с ID={new_id} успешно добавлена в таблицу "{table_name}".'
    
    @handle_db_errors
    @log_time
    def select(
        self,
        table_name: str,
        where_column: Optional[str] = None,
        where_value: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Выбрать записи из таблицы.
        
        Args:
            table_name: Имя таблицы
            where_column: Столбец для фильтрации (опционально)
            where_value: Значение для фильтрации (опционально)
            
        Returns:
            Список записей (словарей)
            
        Raises:
            TableNotFoundError: Если таблица не существует
        """
        # Создаем уникальный ключ для кеширования на основе параметров запроса
        cache_key = f"{table_name}:{where_column}:{where_value}"
        
        # Функция для выполнения запроса
        def _execute_select() -> List[Dict[str, Any]]:
            # Проверить существование таблицы
            table = self.get_table(table_name)
            
            # Загрузить данные таблицы
            table_data = load_table_data(table_name)
            
            # Если нет условия where, вернуть все записи
            if where_column is None or where_value is None:
                return table_data
            
            # Проверить что столбец существует
            try:
                column = table.get_column(where_column)
            except ValueError:
                raise ValidationError(f"Столбец '{where_column}' не найден в таблице")
            
            # Преобразовать значение where в нужный тип
            typed_value = self._validate_value(where_value, column.type)
            
            # Фильтровать записи
            filtered = [
                record
                for record in table_data
                if record.get(where_column) == typed_value
            ]
            
            return filtered
        
        # Использовать кеширование
        return self._cache_select(cache_key, _execute_select)
    
    @handle_db_errors
    def update(
        self,
        table_name: str,
        set_column: str,
        set_value: str,
        where_column: str,
        where_value: str,
    ) -> str:
        """
        Обновить записи в таблице.
        
        Args:
            table_name: Имя таблицы
            set_column: Столбец для обновления
            set_value: Новое значение
            where_column: Столбец для условия
            where_value: Значение условия
            
        Returns:
            Сообщение об успехе
            
        Raises:
            TableNotFoundError: Если таблица не существует
            ValidationError: Если данные некорректны
            RecordNotFoundError: Если записи не найдены
        """
        # Проверить существование таблицы
        table = self.get_table(table_name)
        
        # Проверить что столбцы существуют
        try:
            set_col = table.get_column(set_column)
            where_col = table.get_column(where_column)
        except ValueError as e:
            raise ValidationError(str(e))
        
        # Загрузить данные таблицы
        table_data = load_table_data(table_name)
        
        # Преобразовать значения
        typed_where_value = self._validate_value(where_value, where_col.type)
        typed_set_value = self._validate_value(set_value, set_col.type)
        
        # Найти и обновить записи
        updated_count = 0
        updated_ids = []
        
        for record in table_data:
            if record.get(where_column) == typed_where_value:
                record[set_column] = typed_set_value
                updated_count += 1
                updated_ids.append(record["ID"])
        
        if updated_count == 0:
            raise RecordNotFoundError(
                f"Записи с условием {where_column}={where_value} не найдены"
            )
        
        # Сохранить данные
        save_table_data(table_name, table_data)
        
        # Очистить кеш select-запросов
        self._clear_select_cache()
        
        ids_str = ", ".join([f"ID={id}" for id in updated_ids])
        if updated_count == 1:
            return f'Запись с {ids_str} в таблице "{table_name}" успешно обновлена.'
        else:
            return (
                f'{updated_count} записей ({ids_str}) в таблице "{table_name}" '
                f'успешно обновлены.'
            )
    
    @handle_db_errors
    @confirm_action("удаление записей")
    def delete(
        self, table_name: str, where_column: str, where_value: str
    ) -> str:
        """
        Удалить записи из таблицы.
        
        Args:
            table_name: Имя таблицы
            where_column: Столбец для условия
            where_value: Значение условия
            
        Returns:
            Сообщение об успехе
            
        Raises:
            TableNotFoundError: Если таблица не существует
            ValidationError: Если данные некорректны
            RecordNotFoundError: Если записи не найдены
        """
        # Проверить существование таблицы
        table = self.get_table(table_name)
        
        # Проверить что столбец существует
        try:
            column = table.get_column(where_column)
        except ValueError as e:
            raise ValidationError(str(e))
        
        # Загрузить данные таблицы
        table_data = load_table_data(table_name)
        
        # Преобразовать значение
        typed_value = self._validate_value(where_value, column.type)
        
        # Найти записи для удаления
        to_delete = [
            record
            for record in table_data
            if record.get(where_column) == typed_value
        ]
        
        if not to_delete:
            raise RecordNotFoundError(
                f"Записи с условием {where_column}={where_value} не найдены"
            )
        
        deleted_ids = [record["ID"] for record in to_delete]
        
        # Удалить записи
        table_data = [
            record
            for record in table_data
            if record.get(where_column) != typed_value
        ]
        
        # Сохранить данные
        save_table_data(table_name, table_data)
        
        # Очистить кеш select-запросов
        self._clear_select_cache()
        
        ids_str = ", ".join([f"ID={id}" for id in deleted_ids])
        if len(deleted_ids) == 1:
            return f'Запись с {ids_str} успешно удалена из таблицы "{table_name}".'
        else:
            return (
                f'{len(deleted_ids)} записей ({ids_str}) успешно удалены '
                f'из таблицы "{table_name}".'
            )
    
    @handle_db_errors
    def get_table_info(self, table_name: str) -> str:
        """
        Получить информацию о таблице.
        
        Args:
            table_name: Имя таблицы
            
        Returns:
            Информация о таблице
            
        Raises:
            TableNotFoundError: Если таблица не существует
        """
        table = self.get_table(table_name)
        table_data = load_table_data(table_name)
        
        columns_str = ", ".join([f"{col.name}:{col.type}" for col in table.columns])
        record_count = len(table_data)
        
        return (
            f"Таблица: {table_name}\n"
            f"Столбцы: {columns_str}\n"
            f"Количество записей: {record_count}"
        )
