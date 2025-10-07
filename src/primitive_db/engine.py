import shlex

import prompt

from .commands import DatabaseCommandRegistry, InvalidCommandError
from .core import Database


def print_help():
    """Prints the help message for the current mode."""
    print("\n***Процесс работы с таблицей***")
    print("Функции:")
    print("<command> create_table <имя_таблицы> <столбец1:тип> .. - создать таблицу")
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")


def run():
    """Main function that runs the database engine."""
    print("\n***База данных***")
    print("\nФункции:")
    print(
        "<command> create_table <имя_таблицы> <столбец1:тип> <столбец2:тип> .. "
        "- создать таблицу"
    )
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация")
    
    db = Database()
    command_registry = DatabaseCommandRegistry()
    
    while True:
        user_input = prompt.string("\n>>>Введите команду: ")
        
        if not user_input.strip():
            continue
            
        try:
            args = shlex.split(user_input)
        except ValueError:
            print("Некорректная команда. Попробуйте снова.")
            continue
            
        if not args:
            continue
            
        command_name = args[0].lower()
        
        # Handle non-DB commands directly
        if command_name == "exit":
            break
        elif command_name == "help":
            print_help()
        # Handle DB commands using Command Pattern
        elif command_registry.is_database_command(command_name):
            try:
                command = command_registry.parse_command(user_input)
                result = command.execute(db)
                print(result)
            except InvalidCommandError as e:
                print(str(e))
        else:
            print(f"Функции {command_name} нет. Попробуйте снова.")
