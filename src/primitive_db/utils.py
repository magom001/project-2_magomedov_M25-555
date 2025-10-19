import json
import os
from typing import Any, Dict, List

from .constants import DATA_DIR


def load_metadata(filepath: str) -> dict:
    """
    Load metadata from JSON file.
    
    Args:
        filepath: Path to the JSON file
        
    Returns:
        Dictionary with metadata or empty dict if file not found
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_metadata(filepath: str, data: dict) -> None:
    """
    Save metadata to JSON file.
    
    Args:
        filepath: Path to the JSON file
        data: Dictionary with metadata to save
    """
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_table_data(table_name: str) -> List[Dict[str, Any]]:
    """
    Загрузить данные таблицы из JSON файла.
    
    Args:
        table_name: Имя таблицы
        
    Returns:
        Список записей (словарей) или пустой список если файл не найден
    """
    filepath = os.path.join(DATA_DIR, f"{table_name}.json")
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return []


def save_table_data(table_name: str, data: List[Dict[str, Any]]) -> None:
    """
    Сохранить данные таблицы в JSON файл.
    
    Args:
        table_name: Имя таблицы
        data: Список записей (словарей) для сохранения
    """
    # Создать директорию data если она не существует
    os.makedirs(DATA_DIR, exist_ok=True)
    
    filepath = os.path.join(DATA_DIR, f"{table_name}.json")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def delete_table_data(table_name: str) -> None:
    """
    Удалить файл данных таблицы.
    
    Args:
        table_name: Имя таблицы
    """
    filepath = os.path.join(DATA_DIR, f"{table_name}.json")
    
    try:
        os.remove(filepath)
    except FileNotFoundError:
        pass  # Файл уже не существует