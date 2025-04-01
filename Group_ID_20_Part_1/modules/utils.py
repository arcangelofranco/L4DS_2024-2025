import os
import re
import json
import logging as log
import asyncio
from typing import Dict, Callable

def log_execution(function: Callable):
    """
    A decorator for logging the execution of asynchronous functions.

    Args:
        `function (Callable)`: The asynchronous function to be decorated.

    Returns:
        `Callable`: A wrapper function that logs the execution of the decorated function.
    
    Raises:
        `TypeError`: If the decorated function is not asynchronous.
    """
    if not asyncio.iscoroutinefunction(function):
        raise TypeError("`log_execution` can only decorate asynchronous functions.")

    async def wrapper(*args, **kwargs):
        log.info(f"Running execution: `{function.__name__}`")
        try:
            result = await function(*args, **kwargs)
            log.info(f"Execution completed: `{function.__name__}`")
            return result
        except Exception as e:
            log.error(f"Error in `{function.__name__}`: {e}")
            raise

    return wrapper


def get_root(folder_root: str) -> str:
    """
    Retrieves the root directory path based on the folder name.

    Args:
        `folder_root (str)`: The name of the root folder to locate.

    Returns:
        `str`: The absolute path to the root directory containing the specified folder.

    Raises:
        `KeyError`: If the specified folder is not found in the directory path.
    """
    directory = os.getcwd().split(os.path.sep)
    if folder_root not in directory:
        raise KeyError(f"Folder `{folder_root}` not found in the current path.")
    root_index = directory.index(folder_root)
    return os.path.sep.join(directory[:root_index + 1])


def get_paths(root_path: str, mode: str) -> Dict[str, str]:
    """
    Generate a dictionary mapping file keys to their absolute paths based on the specified mode.

    Args:
        `root_path (str)`: The root directory where the data folder resides.
        `mode (str)`: The mode of the files to retrieve. Must be one of `raw`, `cleaned`, or `splitted`.

    Returns:
        `Dict[str, str]`: A dictionary where the keys represent file identifiers and the values are absolute paths to the files.

    Raises:
        `ValueError`: If the mode is not one of `raw`, `cleaned`, or `splitted`.
    """
    file_maps = {
        "raw": {
            "CRASHES": ("raw", "Crashes.csv"),
            "PEOPLE": ("raw", "People.csv"),
            "VEHICLES": ("raw", "Vehicles.csv"),
            "POLICE_BEAT": ("external", "PoliceBeatDec2012_20241126.csv"),
            "CITY_US": ("external", "UScities.csv"),
        },
        "cleaned": {
            "CRASHES": ("cleaned", "crashes_cleaned.csv"),
            "PEOPLE": ("cleaned", "people_cleaned.csv"),
            "VEHICLES": ("cleaned", "vehicles_cleaned.csv"),
        },
        "splitted": {
            "CRASH": ("splitted", "crash.csv"),
            "DATE": ("splitted", "date.csv"),
            "LOCATION": ("splitted", "location.csv"),
            "INJURY": ("splitted", "injury.csv"),
            "PERSON": ("splitted", "person.csv"),
            "VEHICLE": ("splitted", "vehicle.csv"),
            "DAMAGE": ("splitted", "damage.csv"),
        },
    }

    if mode not in file_maps:
        raise ValueError(f"Invalid mode: `{mode}`. Use `raw`, `cleaned`, or `splitted`.")

    base_directory = os.path.join(root_path, "data")
    file_map = file_maps[mode]

    return {
        key: os.path.join(base_directory, relative_subdir, filename)
        for key, (relative_subdir, filename) in file_map.items()
    }


def read_json(file_path: str) -> Dict[str, str]:
    """
    Reads a `JSON` file and returns its content as a dictionary.

    Args:
       ` file_path (str)`: The path to the JSON file to be read.

    Returns:
        `Dict[str, str]`: The parsed content of the JSON file.

    Raises:
        `FileNotFoundError`: If the file does not exist.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"JSON file `{file_path}` does not exist.")
    with open(file_path, 'r') as file:
        return json.load(file)


def remove_brackets_and_following(text: str) -> str:
    """
    Removes the content inside brackets (including the brackets themselves) from a given text.

    Args:
        `text (str)`: The text from which to remove the brackets and following content.

    Returns:
        `str`: The modified text without brackets and following content.
    """
    return re.sub(r"\(.*", "", text).strip()


def remove_after_symbols(text: str) -> str:
    """
    Removes everything after the first comma, semicolon, or ampersand in a given text.

    Args:
        `text (str)`: The text from which to remove the part after symbols.

    Returns:
        `str`: The modified text with content removed after the specified symbols.
    """
    return re.split(r"[,;&]+", text, maxsplit=1)[0].strip()


def remove_irrelevants(text: str) -> str:
    """
    Removes certain irrelevant words (like company types) from a given text.

    Args:
        `text (str)`: The text from which irrelevant words should be removed.

    Returns:
        `str`: The modified text with irrelevant words removed.
    """
    return re.sub(
        r"\b(?:INC|LTD|CORP|LLC|CO|CA|DIV|MFD|MFG|BY|SALES|FOR|LOFT|TX)\b\.?(\s|,|$)",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip()


def remove_quotes(text: str) -> str:
    """
    Removes all quotes (single and double) from a given text.

    Args:
        `text (str)`: The text from which to remove quotes.

    Returns:
        `str`: The modified text without quotes.
    """
    return re.sub(r"[\"']", "", text).strip()


def handle_punctation(text: str, replace: bool = False, placer: str = None) -> str:
    """
    Handles punctuation marks (comma and semicolon) by either removing or replacing them.

    Args:
        `text (str)`: The text where punctuation should be handled.
        `replace (bool)`: If True, replace the punctuation marks with the provided `placer`. Default is False (removes punctuation).
        `placer (str)`: The string to replace the punctuation marks with, used if `replace` is True.

    Returns:
        `str`: The modified text with punctuation handled according to the `replace` parameter.
    """
    if replace:
        return re.sub(r"[,;]", f"{placer}", text).strip()
    return re.sub(r"[,;]", "", text).strip()
