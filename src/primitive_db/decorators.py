"""Декораторы для улучшения функциональности базы данных."""

import time
from functools import wraps
from typing import Any, Callable

import prompt


def handle_db_errors(func: Callable) -> Callable:
    """
    Декоратор для централизованной обработки ошибок базы данных.
    
    Перехватывает и обрабатывает стандартные исключения БД,
    предоставляя понятные сообщения об ошибках.
    
    Args:
        func: Функция для декорирования
        
    Returns:
        Обёрнутая функция с обработкой ошибок
    """
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        # Import at runtime to avoid circular import
        from .core import (
            DatabaseError,
            RecordNotFoundError,
            TableExistsError,
            TableNotFoundError,
            ValidationError,
        )
        
        try:
            return func(*args, **kwargs)
        except FileNotFoundError as e:
            return (
                f"Ошибка: Файл данных не найден. "
                f"Возможно, база данных не инициализирована. ({e})"
            )
        except TableExistsError as e:
            return str(e)
        except TableNotFoundError as e:
            return str(e)
        except ValidationError as e:
            return f"Ошибка валидации: {e}"
        except RecordNotFoundError as e:
            return str(e)
        except KeyError as e:
            return f"Ошибка: Таблица или столбец {e} не найден."
        except ValueError as e:
            return f"Ошибка значения: {e}"
        except DatabaseError as e:
            return f"Ошибка базы данных: {e}"
        except Exception as e:
            return f"Произошла непредвиденная ошибка: {e}"
    
    return wrapper


def confirm_action(action_name: str) -> Callable:
    """
    Фабрика декораторов для запроса подтверждения опасных операций.
    
    Args:
        action_name: Название действия для отображения в запросе
        
    Returns:
        Декоратор для применения к функции
        
    Example:
        @confirm_action("удаление таблицы")
        def drop_table(table_name):
            # ... логика удаления
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Запросить подтверждение
            confirmation = prompt.string(
                f'Вы уверены, что хотите выполнить "{action_name}"? [y/n]: '
            )
            
            if confirmation.lower() not in ('y', 'yes', 'д', 'да'):
                return f"Операция '{action_name}' отменена."
            
            return func(*args, **kwargs)
        
        return wrapper
    
    return decorator


def log_time(func: Callable) -> Callable:
    """
    Декоратор для замера времени выполнения функции.
    
    Измеряет и выводит время выполнения функции в консоль.
    Использует time.monotonic() для точного измерения.
    
    Args:
        func: Функция для декорирования
        
    Returns:
        Обёрнутая функция с замером времени
    """
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start_time = time.monotonic()
        result = func(*args, **kwargs)
        end_time = time.monotonic()
        
        elapsed = end_time - start_time
        print(f"Функция {func.__name__} выполнилась за {elapsed:.3f} секунд.")
        
        return result
    
    return wrapper


def create_cacher() -> Callable:
    """
    Создаёт функцию кэширования с замыканием.
    
    Возвращает функцию cache_result, которая хранит кэш
    в своём замыкании и использует его для оптимизации
    повторяющихся запросов.
    
    Returns:
        Функция cache_result(key, value_func)
        
    Example:
        cache = create_cacher()
        result = cache("users_all", lambda: db.select("users"))
    """
    cache = {}
    
    def cache_result(key: str, value_func: Callable) -> Any:
        """
        Получить результат из кэша или вычислить его.
        
        Args:
            key: Ключ для кэширования
            value_func: Функция для получения значения, если его нет в кэше
            
        Returns:
            Закэшированный или новый результат
        """
        if key in cache:
            print(f"[CACHE HIT] Используется кэшированный результат для '{key}'")
            return cache[key]
        
        print(f"[CACHE MISS] Вычисляется результат для '{key}'")
        result = value_func()
        cache[key] = result
        return result
    
    def clear_cache(key: str = None) -> None:
        """
        Очистить кэш полностью или для определённого ключа.
        
        Args:
            key: Ключ для очистки (если None, очищается весь кэш)
        """
        if key is None:
            cache.clear()
            print("[CACHE] Весь кэш очищен")
        elif key in cache:
            del cache[key]
            print(f"[CACHE] Очищен кэш для '{key}'")
    
    # Добавляем метод для очистки кэша
    cache_result.clear = clear_cache
    
    return cache_result
