import shlex

import prompt

from .commands import DatabaseCommandRegistry, InvalidCommandError
from .core import Database


def print_welcome():
    """Prints the welcome message with all available commands."""
    print("\n***База данных***")
    print("\n***Операции с данными***")
    print("\nФункции:")
    print(
        "<command> insert into <имя_таблицы> values (<значение1>, <значение2>, ...) "
        "- создать запись"
    )
    print(
        "<command> select from <имя_таблицы> where <столбец> = <значение> "
        "- прочитать записи по условию"
    )
    print("<command> select from <имя_таблицы> - прочитать все записи")
    print(
        "<command> update <имя_таблицы> set <столбец> = <новое_значение> "
        "where <столбец> = <значение> - обновить запись"
    )
    print(
        "<command> delete from <имя_таблицы> where <столбец> = <значение> "
        "- удалить запись"
    )
    print("<command> info <имя_таблицы> - вывести информацию о таблице")
    print("\nУправление таблицами:")
    print(
        "<command> create_table <имя_таблицы> <столбец1:тип> <столбец2:тип> .. "
        "- создать таблицу"
    )
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")


def print_help():
    """Prints the help message for the current mode."""
    print_welcome()


def run():
    """Main function that runs the database engine."""
    print_welcome()
    
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
