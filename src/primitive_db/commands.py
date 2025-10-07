import shlex
from abc import ABC, abstractmethod
from typing import Dict, List, Type

from .core import Database, TableExistsError, TableNotFoundError


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