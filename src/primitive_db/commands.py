import shlex
from abc import ABC, abstractmethod
from typing import Dict, List, Type

from prettytable import PrettyTable

from .core import (
    Database,
    RecordNotFoundError,
    TableExistsError,
    TableNotFoundError,
    ValidationError,
)
from .parser import (
    ParseError,
    parse_delete,
    parse_info,
    parse_insert,
    parse_select,
    parse_update,
)


class InvalidCommandError(Exception):
    """Исключение для некорректных команд."""
    pass


class DatabaseCommand(ABC):
    """Абстрактный базовый класс для команд базы данных."""
    
    @abstractmethod
    def execute(self, db: Database) -> str:
        """
        Выполнить команду в базе данных.
        
        Args:
            db: Экземпляр базы данных для операций
            
        Returns:
            Сообщение с результатом для отображения пользователю
        """
        pass
    
    @classmethod
    @abstractmethod
    def from_input(cls, args: List[str]) -> "DatabaseCommand":
        """
        Создать экземпляр команды из пользовательского ввода.
        
        Args:
            args: Список аргументов команды (первый элемент - имя команды)
            
        Returns:
            Экземпляр команды
            
        Raises:
            InvalidCommandError: Если ввод некорректен для данной команды
        """
        pass
    
    @classmethod
    @abstractmethod
    def get_command_name(cls) -> str:
        """Получить имя команды, которое запускает данную команду."""
        pass


class CreateTableCommand(DatabaseCommand):
    """Команда для создания новой таблицы."""
    
    def __init__(self, table_name: str, column_specs: List[str]):
        self.table_name = table_name
        self.column_specs = column_specs
    
    def execute(self, db: Database) -> str:
        """Создать таблицу в базе данных."""
        try:
            return db.create_table(self.table_name, self.column_specs)
        except (TableExistsError, ValueError) as e:
            return str(e)
    
    @classmethod
    def from_input(cls, args: List[str]) -> "CreateTableCommand":
        """Разобрать аргументы команды create_table."""
        if len(args) < 3:
            raise InvalidCommandError(
                "Некорректное значение: недостаточно аргументов. Попробуйте снова."
            )
        
        command_name = args[0]
        if command_name.lower() != cls.get_command_name():
            raise InvalidCommandError(f"Неверное имя команды: {command_name}")
        
        table_name = args[1]
        column_specs = args[2:]
        
        return cls(table_name, column_specs)
    
    @classmethod
    def get_command_name(cls) -> str:
        return "create_table"


class DropTableCommand(DatabaseCommand):
    """Команда для удаления таблицы."""
    
    def __init__(self, table_name: str):
        self.table_name = table_name
    
    def execute(self, db: Database) -> str:
        """Удалить таблицу из базы данных."""
        try:
            return db.drop_table(self.table_name)
        except TableNotFoundError as e:
            return str(e)
    
    @classmethod
    def from_input(cls, args: List[str]) -> "DropTableCommand":
        """Разобрать аргументы команды drop_table."""
        if len(args) != 2:
            raise InvalidCommandError(
                "Некорректное значение: неправильное количество аргументов. "
                "Попробуйте снова."
            )
        
        command_name = args[0]
        if command_name.lower() != cls.get_command_name():
            raise InvalidCommandError(f"Неверное имя команды: {command_name}")
        
        table_name = args[1]
        return cls(table_name)
    
    @classmethod
    def get_command_name(cls) -> str:
        return "drop_table"


class ListTablesCommand(DatabaseCommand):
    """Команда для отображения списка всех таблиц."""
    
    def execute(self, db: Database) -> str:
        """Получить список всех таблиц в базе данных."""
        return db.list_tables()
    
    @classmethod
    def from_input(cls, args: List[str]) -> "ListTablesCommand":
        """Разобрать аргументы команды list_tables."""
        if len(args) != 1:
            raise InvalidCommandError(
                "Некорректное значение: команда list_tables не принимает "
                "аргументов. Попробуйте снова."
            )
        
        command_name = args[0]
        if command_name.lower() != cls.get_command_name():
            raise InvalidCommandError(f"Неверное имя команды: {command_name}")
        
        return cls()
    
    @classmethod
    def get_command_name(cls) -> str:
        return "list_tables"


class DatabaseCommandRegistry:
    """Реестр для команд базы данных."""
    
    def __init__(self):
        self._commands: Dict[str, Type[DatabaseCommand]] = {}
        self._register_default_commands()
    
    def _register_default_commands(self):
        """Зарегистрировать все команды базы данных по умолчанию."""
        self.register_command(CreateTableCommand)
        self.register_command(DropTableCommand)
        self.register_command(ListTablesCommand)
        self.register_command(InsertCommand)
        self.register_command(SelectCommand)
        self.register_command(UpdateCommand)
        self.register_command(DeleteCommand)
        self.register_command(InfoCommand)
    
    def register_command(self, command_class: Type[DatabaseCommand]):
        """Зарегистрировать класс команды базы данных."""
        command_name = command_class.get_command_name()
        self._commands[command_name] = command_class
    
    def is_database_command(self, command_name: str) -> bool:
        """Проверить, является ли имя команды командой базы данных."""
        return command_name.lower() in self._commands
    
    def parse_command(self, user_input: str) -> DatabaseCommand:
        """
        Разобрать пользовательский ввод и создать соответствующую команду базы данных.
        
        Args:
            user_input: Строка с пользовательским вводом
            
        Returns:
            Экземпляр команды базы данных
            
        Raises:
            InvalidCommandError: Если команда недействительна
        """
        if not user_input.strip():
            raise InvalidCommandError("Пустая команда")
        
        try:
            args = shlex.split(user_input)
        except ValueError:
            raise InvalidCommandError("Некорректная команда. Попробуйте снова.")
        
        if not args:
            raise InvalidCommandError("Пустая команда")
        
        command_name = args[0].lower()
        
        if not self.is_database_command(command_name):
            raise InvalidCommandError(f"Неизвестная команда: {command_name}")
        
        command_class = self._commands[command_name]
        return command_class.from_input(args)
    
    def get_database_commands(self) -> List[str]:
        """Получить список имён доступных команд базы данных."""
        return list(self._commands.keys())


class InsertCommand(DatabaseCommand):
    """Команда для вставки записи в таблицу."""
    
    def __init__(self, table_name: str, values: List[str]):
        self.table_name = table_name
        self.values = values
    
    def execute(self, db: Database) -> str:
        """Вставить запись в таблицу."""
        try:
            return db.insert(self.table_name, self.values)
        except (TableNotFoundError, ValidationError) as e:
            return str(e)
    
    @classmethod
    def from_input(cls, args: List[str]) -> "InsertCommand":
        """Разобрать команду insert from user input."""
        # Объединить обратно в строку для парсера
        command = " ".join(args)
        
        try:
            table_name, values = parse_insert(command)
            return cls(table_name, values)
        except ParseError as e:
            raise InvalidCommandError(str(e))
    
    @classmethod
    def get_command_name(cls) -> str:
        return "insert"


class SelectCommand(DatabaseCommand):
    """Команда для выборки записей из таблицы."""
    
    def __init__(
        self,
        table_name: str,
        where_column: str = None,
        where_value: str = None,
    ):
        self.table_name = table_name
        self.where_column = where_column
        self.where_value = where_value
    
    def execute(self, db: Database) -> str:
        """Выбрать записи из таблицы."""
        try:
            records = db.select(
                self.table_name, self.where_column, self.where_value
            )
            
            if not records:
                return f'В таблице "{self.table_name}" нет записей.'
            
            # Получить таблицу для определения столбцов
            table = db.get_table(self.table_name)
            column_names = [col.name for col in table.columns]
            
            # Создать PrettyTable
            pt = PrettyTable()
            pt.field_names = column_names
            
            # Добавить записи
            for record in records:
                row = [record.get(col, "") for col in column_names]
                pt.add_row(row)
            
            return str(pt)
        except (TableNotFoundError, ValidationError) as e:
            return str(e)
    
    @classmethod
    def from_input(cls, args: List[str]) -> "SelectCommand":
        """Разобрать команду select from user input."""
        # Объединить обратно в строку для парсера
        command = " ".join(args)
        
        try:
            table_name, where_column, where_value = parse_select(command)
            return cls(table_name, where_column, where_value)
        except ParseError as e:
            raise InvalidCommandError(str(e))
    
    @classmethod
    def get_command_name(cls) -> str:
        return "select"


class UpdateCommand(DatabaseCommand):
    """Команда для обновления записей в таблице."""
    
    def __init__(
        self,
        table_name: str,
        set_column: str,
        set_value: str,
        where_column: str,
        where_value: str,
    ):
        self.table_name = table_name
        self.set_column = set_column
        self.set_value = set_value
        self.where_column = where_column
        self.where_value = where_value
    
    def execute(self, db: Database) -> str:
        """Обновить записи в таблице."""
        try:
            return db.update(
                self.table_name,
                self.set_column,
                self.set_value,
                self.where_column,
                self.where_value,
            )
        except (
            TableNotFoundError,
            ValidationError,
            RecordNotFoundError,
        ) as e:
            return str(e)
    
    @classmethod
    def from_input(cls, args: List[str]) -> "UpdateCommand":
        """Разобрать команду update from user input."""
        # Объединить обратно в строку для парсера
        command = " ".join(args)
        
        try:
            (
                table_name,
                set_column,
                set_value,
                where_column,
                where_value,
            ) = parse_update(command)
            return cls(
                table_name, set_column, set_value, where_column, where_value
            )
        except ParseError as e:
            raise InvalidCommandError(str(e))
    
    @classmethod
    def get_command_name(cls) -> str:
        return "update"


class DeleteCommand(DatabaseCommand):
    """Команда для удаления записей из таблицы."""
    
    def __init__(self, table_name: str, where_column: str, where_value: str):
        self.table_name = table_name
        self.where_column = where_column
        self.where_value = where_value
    
    def execute(self, db: Database) -> str:
        """Удалить записи из таблицы."""
        try:
            return db.delete(self.table_name, self.where_column, self.where_value)
        except (
            TableNotFoundError,
            ValidationError,
            RecordNotFoundError,
        ) as e:
            return str(e)
    
    @classmethod
    def from_input(cls, args: List[str]) -> "DeleteCommand":
        """Разобрать команду delete from user input."""
        # Объединить обратно в строку для парсера
        command = " ".join(args)
        
        try:
            table_name, where_column, where_value = parse_delete(command)
            return cls(table_name, where_column, where_value)
        except ParseError as e:
            raise InvalidCommandError(str(e))
    
    @classmethod
    def get_command_name(cls) -> str:
        return "delete"


class InfoCommand(DatabaseCommand):
    """Команда для получения информации о таблице."""
    
    def __init__(self, table_name: str):
        self.table_name = table_name
    
    def execute(self, db: Database) -> str:
        """Получить информацию о таблице."""
        try:
            return db.get_table_info(self.table_name)
        except TableNotFoundError as e:
            return str(e)
    
    @classmethod
    def from_input(cls, args: List[str]) -> "InfoCommand":
        """Разобрать команду info from user input."""
        # Объединить обратно в строку для парсера
        command = " ".join(args)
        
        try:
            table_name = parse_info(command)
            return cls(table_name)
        except ParseError as e:
            raise InvalidCommandError(str(e))
    
    @classmethod
    def get_command_name(cls) -> str:
        return "info"