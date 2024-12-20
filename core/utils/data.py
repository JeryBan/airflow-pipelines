"""
Utility functions to manipulate data.
"""
import csv
from pathlib import Path

from core.share import DIRECTORIES

DATA_DIR = DIRECTORIES.DATA
TEMP_DIR = DIRECTORIES.TEMP


def export_xls_from_base64(data, filename=None):
    """
    Creates xls from data encoded in base64.
    """
    import base64

    save_dir = TEMP_DIR
    path = save_dir / filename

    decoded_data = base64.b64decode(data)
    with open(path, 'wb') as f:
        f.write(decoded_data)
        f.close()

    return str(path)


def get_or_blank(item, value, default=''):
    """
    Safely get a value from a dictionary-like object, returning a default if the key is missing or the value is None.

    This function attempts to retrieve a value from the given item using the specified key (value).
    If the key is missing or the corresponding value is None, it returns the default value instead.

    Args:
        item (dict-like): The dictionary-like object to retrieve the value from.
        value (hashable): The key to look up in the dictionary-like object.
        default (Any, optional): The value to return if the key is missing or the value is None. Defaults to an empty string.

    Returns:
        Any: The value associated with the key in the dictionary-like object, or the default value if the key is missing or the value is None.

    Raises:
        AttributeError: If 'item' is None or doesn't have a 'get' method.
        TypeError: If 'value' is not a hashable type (i.e., can't be used as a dictionary key).

    Examples:
        >>> d = {'a': 1, 'b': None, 'c': 0}
        >>> get_or_blank(d, 'a')
        1
        >>> get_or_blank(d, 'b')
        ''
        >>> get_or_blank(d, 'c')
        0
        >>> get_or_blank(d, 'd')
        ''
        >>> get_or_blank(d, 'd', default='Not found')
        'Not found'
        >>> get_or_blank(None, 'a')
        ''
    """
    if item is None or not hasattr(item, 'get'):
        return default
    result = item.get(value, default)
    return result if result is not None else default
