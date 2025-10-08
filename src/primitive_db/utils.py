import json


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