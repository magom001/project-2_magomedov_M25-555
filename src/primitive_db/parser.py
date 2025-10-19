"""Парсер для SQL-подобных команд."""

import re
import shlex
from typing import List, Optional, Tuple


class ParseError(Exception):
    """Исключение при ошибке парсинга команды."""
    pass


def parse_insert(command: str) -> Tuple[str, List[str]]:
    """
    Парсинг команды INSERT INTO.
    
    Формат: insert into <table_name> values (<value1>, <value2>, ...)
    
    Args:
        command: Строка команды
        
    Returns:
        Кортеж (имя_таблицы, список_значений)
        
    Raises:
        ParseError: Если формат команды некорректен
    """
    # Паттерн: insert into <table> values (...)
    pattern = r'insert\s+into\s+(\w+)\s+values\s*\((.*)\)'
    match = re.match(pattern, command, re.IGNORECASE)
    
    if not match:
        raise ParseError(
            "Некорректный формат команды INSERT. "
            "Используйте: insert into <таблица> values (<значение1>, <значение2>, ...)"
        )
    
    table_name = match.group(1)
    values_str = match.group(2)
    
    # Парсинг значений с учетом кавычек
    try:
        values = shlex.split(values_str.replace(',', ' '))
    except ValueError as e:
        raise ParseError(f"Ошибка парсинга значений: {e}")
    
    if not values:
        raise ParseError("Не указаны значения для вставки")
    
    return table_name, values


def parse_select(command: str) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Парсинг команды SELECT FROM.
    
    Форматы: 
    - select from <table_name>
    - select from <table_name> where <column> = <value>
    
    Args:
        command: Строка команды
        
    Returns:
        Кортеж (имя_таблицы, столбец_where, значение_where)
        
    Raises:
        ParseError: Если формат команды некорректен
    """
    # Паттерн с WHERE
    pattern_with_where = (
        r'select\s+from\s+(\w+)\s+where\s+(\w+)\s*=\s*(.+)'
    )
    match = re.match(pattern_with_where, command, re.IGNORECASE)
    
    if match:
        table_name = match.group(1)
        where_column = match.group(2)
        where_value = match.group(3).strip()
        
        # Убрать кавычки если есть
        if where_value.startswith('"') and where_value.endswith('"'):
            where_value = where_value[1:-1]
        
        return table_name, where_column, where_value
    
    # Паттерн без WHERE
    pattern_no_where = r'select\s+from\s+(\w+)'
    match = re.match(pattern_no_where, command, re.IGNORECASE)
    
    if match:
        table_name = match.group(1)
        return table_name, None, None
    
    raise ParseError(
        "Некорректный формат команды SELECT. "
        "Используйте: select from <таблица> [where <столбец> = <значение>]"
    )


def parse_update(command: str) -> Tuple[str, str, str, str, str]:
    """
    Парсинг команды UPDATE.
    
    Формат: update <table_name> set <column> = <value> where <column> = <value>
    
    Args:
        command: Строка команды
        
    Returns:
        Кортеж (имя_таблицы, set_столбец, set_значение, where_столбец, where_значение)
        
    Raises:
        ParseError: Если формат команды некорректен
    """
    pattern = (
        r'update\s+(\w+)\s+set\s+(\w+)\s*=\s*(.+?)\s+where\s+(\w+)\s*=\s*(.+)'
    )
    match = re.match(pattern, command, re.IGNORECASE)
    
    if not match:
        raise ParseError(
            "Некорректный формат команды UPDATE. "
            "Используйте: update <таблица> set <столбец> = <значение> "
            "where <столбец> = <значение>"
        )
    
    table_name = match.group(1)
    set_column = match.group(2)
    set_value = match.group(3).strip()
    where_column = match.group(4)
    where_value = match.group(5).strip()
    
    # Убрать кавычки если есть
    if set_value.startswith('"') and set_value.endswith('"'):
        set_value = set_value[1:-1]
    if where_value.startswith('"') and where_value.endswith('"'):
        where_value = where_value[1:-1]
    
    return table_name, set_column, set_value, where_column, where_value


def parse_delete(command: str) -> Tuple[str, str, str]:
    """
    Парсинг команды DELETE FROM.
    
    Формат: delete from <table_name> where <column> = <value>
    
    Args:
        command: Строка команды
        
    Returns:
        Кортеж (имя_таблицы, where_столбец, where_значение)
        
    Raises:
        ParseError: Если формат команды некорректен
    """
    pattern = r'delete\s+from\s+(\w+)\s+where\s+(\w+)\s*=\s*(.+)'
    match = re.match(pattern, command, re.IGNORECASE)
    
    if not match:
        raise ParseError(
            "Некорректный формат команды DELETE. "
            "Используйте: delete from <таблица> where <столбец> = <значение>"
        )
    
    table_name = match.group(1)
    where_column = match.group(2)
    where_value = match.group(3).strip()
    
    # Убрать кавычки если есть
    if where_value.startswith('"') and where_value.endswith('"'):
        where_value = where_value[1:-1]
    
    return table_name, where_column, where_value


def parse_info(command: str) -> str:
    """
    Парсинг команды INFO.
    
    Формат: info <table_name>
    
    Args:
        command: Строка команды
        
    Returns:
        Имя таблицы
        
    Raises:
        ParseError: Если формат команды некорректен
    """
    pattern = r'info\s+(\w+)'
    match = re.match(pattern, command, re.IGNORECASE)
    
    if not match:
        raise ParseError(
            "Некорректный формат команды INFO. "
            "Используйте: info <таблица>"
        )
    
    return match.group(1)
