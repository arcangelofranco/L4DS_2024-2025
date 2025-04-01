import csv
import aiofiles
import geohash

from datetime import datetime
from Levenshtein import distance as levensthein_distance
from typing import Any, List, Dict, Callable, Tuple

from modules.reader import Reader

class Column:
    """Handles column-level operations on a dataset."""
    def __init__(self, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
        """Initializes a Column instance.

        Args:
            `rows (List[Dict[str, Any]])`: The data rows.
            `fieldnames (List[str])`: The column headers.
        """
        self.rows = rows
        self.fieldnames = fieldnames

    def replace_column_values(self, column: str, condition: Callable[[Any], bool], new_value: Any) -> None:
        """Replaces values in a column based on a condition.

        Args:
            `column (str)`: The column to modify.
            `condition (Callable[[Any], bool])`: A function to evaluate whether to replace a value.
            `new_value (Any)`: The value or function to apply as the replacement.

        Raises:
            `KeyError`: If the column does not exist.
        """
        if column not in self.fieldnames:
            raise KeyError(f"The column `{column}` is not present.")
        for row in self.rows:
            if condition(row.get(column)):
                row[column] = new_value(row[column]) if callable(new_value) else new_value


    def add_column(self, column: str, default_value: Any = None, enum: bool = False) -> None:
        """Adds a new column to the dataset.

        Args:
            `column (str)`: The name of the new column.
            `default_value (Any)`: The default value or function to populate the column.
            `enum (bool)`: Whether to enumerate values using a function.

        Raises:
            `KeyError`: If the column already exists.
        """
        if column in self.fieldnames:
            raise KeyError(f"The column '{column}' is already present.")
        
        self.fieldnames.append(column)
        
        if enum:
            for idx, row in enumerate(self.rows):
                row[column] = default_value(row, idx) if callable(default_value) else default_value
        else:
            for row in self.rows:
                row[column] = default_value(row) if callable(default_value) else default_value


    def remove_columns(self, columns: List[str]) -> None:
        """Removes specified columns from the dataset.

        Args:
            `columns (List[str])`: The columns to remove.

        Raises:
            `KeyError`: If any of the columns do not exist.
        """
        missing_columns = [col for col in columns if col not in self.fieldnames]
        if missing_columns:
            raise KeyError(f"The following columns are not present: {', '.join(missing_columns)}")

        for column in columns:
            if column in self.fieldnames:
                self.fieldnames.remove(column)
                for row in self.rows:
                    row.pop(column, None)

    def rename_column(self, old: str, new: str) -> None:
        """Renames a column in the dataset.

        Args:
            `old (str)`: The current name of the column.
            `new (str)`: The new name for the column.

        Raises:
            `KeyError`: If the old column does not exist or the new column already exists.
        """
        if old not in self.fieldnames:
            raise KeyError(f"The column '{old}' is not present.")
        if new in self.fieldnames:
            raise KeyError(f"The column '{new}' is already present.")

        self.fieldnames = [new if col == old else col for col in self.fieldnames]
        for row in self.rows:
            row[new] = row.pop(old)

    def cast_column(self, column: str, conv_type: type) -> None:
        """Casts the values in a specified column to a given type.

        Args:
            `column (str)`: The name of the column whose values need to be casted.
            `conv_type (type)`: The type to which the column values should be converted.

        Raises:
           `ValueError`: If any value in the column cannot be casted to the specified type.
        """
        for row in self.rows:
            try:
                value = row[column]
                if value:
                    if isinstance(value, str) and '.' in value and value.replace('.', '').isdigit():
                        value = float(value)
                    row[column] = conv_type(value)
                else:
                    row[column] = None
            except Exception:
                raise ValueError(f"Error converting '{column}' column: Value '{row[column]}' invalid.")


    def update_columns(self, columns: List[str]) -> None:
        """Updates the dataset to include only specified columns.

        Args:
            `columns (List[str])`: The columns to retain.

        Raises:
            `KeyError`: If any of the columns do not exist.
        """
        missing_columns = [col for col in columns if col not in self.fieldnames]
        if missing_columns:
            raise KeyError(f"The following columns are not present: {', '.join(missing_columns)}")

        self.fieldnames = columns
        for row in self.rows:
            removed = [key for key in row.keys() if key not in self.fieldnames]
            for key in removed:
                row.pop(key, None)

            ordered = {key: row.get(key) for key in self.fieldnames}
            row.clear()
            row.update(ordered)

class Row:
    """Handles row-level operations on a dataset."""
    def __init__(self, rows: List[Dict[str, Any]], fieldnames: List[str]):
        """Initializes a Row instance.

        Args:
            `rows (List[Dict[str, Any]])`: The data rows.
            `fieldnames (List[str])`: The column headers.
        """
        self.rows = rows
        self.fieldnames = fieldnames

    def filter_rows(self, column: str, condition: Callable[[Dict[str, str]], bool] = None) -> None:
        """Filters rows based on a condition applied to a column.

        Args:
            `column (str)`: The column to evaluate.
            `condition (Callable[[Dict[str, str]], bool], optional)`: The condition function. Defaults to None.

        Raises:
            `KeyError`: If the column does not exist.
        """
        if column not in self.fieldnames:
            raise KeyError(f"The column '{column}' is not present.")

        if condition is None:
            condition = lambda x: not x

        self.rows = [row for row in self.rows if not condition(row.get(column))]


class Data(Reader, Column, Row):
    """
    A class for managing and processing tabular data, including operations
    like data initialization, copying, column manipulation, and geohashing.

    Attributes:
        `input_file (str)`: Path to the input file for initializing data.
        `rows (List)`: List of rows representing the dataset.
        `fieldnames (List)`: List of column names in the dataset.
        `city_state_mapping (Dict[str, str])`: Dictionary mapping city names to state information.
    """

    def __init__(self, input_file: str = None) -> None:
        """
        Initialize a Data object.

        Args:
            `input_file (str, optional)`: Path to the input file to initialize the data. Defaults to None.
        """
        super().__init__()
        self.rows = []
        self.fieldnames = []
        self.city_state_mapping: Dict[str, str] = {}
        self.input_file = input_file

    async def initialize(self) -> None:
        """
        Initialize the data by loading it from the input file if provided.
        """
        if self.input_file:
            await self.load_data(self.input_file)

    def copy(self) -> "Data":
        """
        Create a deep copy of the Data object.

        Returns:
            `Data`: A new instance of the Data class with copied data.
        """
        copy_instance = Data.__new__(Data)
        copy_instance.rows = [row.copy() for row in self.rows]
        copy_instance.fieldnames = self.fieldnames.copy()
        copy_instance.city_state_mapping = getattr(self, 'city_state_mapping', {}).copy()
        return copy_instance

    async def load_city_state(self, city_file: str) -> None:
        """
        Load city and state information from a CSV file into the city_state_mapping attribute.

        Args:
            `city_file (str)`: Path to the city file.

        Raises:
            `Exception`: If an error occurs while loading the city file.
        """
        try:
            async with aiofiles.open(city_file, mode="r", encoding="utf-8") as file:
                content = await file.read()
                reader = csv.DictReader(content.splitlines())
                for row in reader:
                    city = row["city"].lower()
                    self.city_state_mapping[city] = {
                        "city": row["city"].lower(),
                        "state_id": row["state_id"],
                    }
        except Exception as e:
            raise Exception(f"Error during city file loading: {e}")

    def replace_central_tendency(self, column: str, method: str = "mean") -> None:
        """
        Replace missing or invalid values in a column with the specified central tendency (mean or median).

        Args:
            `column (str)`: The column to process.
            `method (str, optional)`: The method for central tendency ("mean" or "median"). Defaults to "mean".

        Raises:
            `ValueError`: If no valid numeric values are found in the column.
            `KeyError`: If the method is not supported.
        """
        valid_values = self.get_valid_values(column)
        if not valid_values:
            raise ValueError(f"No valid numeric value in column {column}.")

        if method == "mean":
            replacement = self._compute_mean(valid_values)
        elif method == "median":
            replacement = self._compute_median(valid_values)
        else:
            raise KeyError(f"Method {method} not supported. Use 'mean' or 'median'.")

        for row in self.rows:
            if row[column] is not row[column] or not self._is_valid_number(row[column]):
                row[column] = replacement

    def get_valid_values(self, column: str) -> List[float]:
        """
        Retrieve valid numeric values from a specified column.

        Args:
            `column (str)`: The column to process.

        Returns:
            `List[float]`: A list of valid numeric values.
        """
        return [
            float(row[column]) for row in self.rows
            if self._is_valid_number(row[column])
        ]

    def correct_city(self, city: str) -> Tuple[str, str]:
        """
        Correct and validate a city name using city_state_mapping and Levenshtein correction.

        Args:
            `city (str)`: The city name to correct.

        Returns:
            `Tuple[str, str]`: Corrected city name and state ID, or ('UNKNOWN', 'XX') if not found.
        """
        city = city.strip().lower()

        if city in self.city_state_mapping:
            return (
                self.city_state_mapping[city]["city"].upper(),
                self.city_state_mapping[city]["state_id"]
            )

        proper_city = self.levenshtein_correction(city)
        if proper_city:
            proper_city = proper_city.strip().lower()
            if proper_city in self.city_state_mapping:
                return (
                    self.city_state_mapping[proper_city]["city"].upper(),
                    self.city_state_mapping[proper_city]["state_id"]
                )

        return ("UNKNOWN", "XX")

    def levenshtein_correction(self, city: str) -> str:
        """
        Find the closest city name in city_state_mapping using Levenshtein distance.

        Args:
            `city (str)`: The city name to match.

        Returns:
            `str`: The closest matching city name, or None if no close match is found.
        """
        closest_match = None
        min_distance = float("inf")

        for proper_city in self.city_state_mapping.keys():
            dist = levensthein_distance(city, proper_city)
            if dist < min_distance:
                min_distance = dist
                closest_match = proper_city

        if min_distance <= 3:
            return self.city_state_mapping[closest_match]["city"]

        return None

    def split_datetime(self, column: str, date_format: str = "%m/%d/%Y %I:%M:%S %p") -> None:
        """
        Split a datetime column into separate columns for date and time components.

        Args:
            `column (str)`: The datetime column to split.
            `date_format (str, optional)`: The format of the datetime values. Defaults to "%m/%d/%Y %I:%M:%S %p".

        Raises:
            `KeyError`: If the specified column is not present.
            `Exception`: If an error occurs during datetime processing.
        """
        if column not in self.fieldnames:
            raise KeyError(f"The column '{column}' is not present.")

        new_columns = [
            "CRASH_MONTH", "CRASH_DAY", "CRASH_YEAR", "CRASH_TIME", "CRASH_PERIOD", "CRASH_SEASON"
        ]

        for col in new_columns:
            self.add_column(col)

        month_to_season = {
            12: "Winter", 1: "Winter", 2: "Winter",
            3: "Spring", 4: "Spring", 5: "Spring",
            6: "Summer", 7: "Summer", 8: "Summer",
            9: "Autumn", 10: "Autumn", 11: "Autumn"
        }

        for row in self.rows:
            try:
                dt = datetime.strptime(row[column], date_format)

                row.update({
                    "CRASH_MONTH": f"{dt.month:02d}",
                    "CRASH_DAY": f"{dt.day:02d}",
                    "CRASH_YEAR": str(dt.year),
                    "CRASH_TIME": dt.strftime("%I:%M:%S"),
                    "CRASH_PERIOD": dt.strftime("%p"),
                    "CRASH_SEASON": month_to_season[dt.month].upper()
                })
            except Exception as e:
                raise Exception(f"Something went wrong: {e}")

    def get_geohash(self, latitude: float, longitude: float, precision: int = 12) -> str:
        """
        Generate a geohash string for the specified latitude and longitude.

        Args:
            `latitude (float)`: The latitude value.
            `longitude (float)`: The longitude value.
            `precision (int, optional)`: The geohash precision level. Defaults to 12.

        Returns:
            `str`: The geohash string.

        Raises:
            `ValueError`: If the latitude or longitude values are invalid.
        """
        if not self._is_valid_number(latitude) or not self._is_valid_number(longitude):
            raise ValueError("Invalid latitude or longitude value.")

        return geohash.encode(latitude, longitude, precision)

    def enhance_data(self, rename_mapping: Dict[str, str], new_columns: Dict[str, Callable[[Dict, int], str]]) -> None:
        """
        Enhance the dataset by renaming columns and adding new computed columns.

        Args:
            `rename_mapping (Dict[str, str])`: Mapping of old column names to new names.
            `new_columns (Dict[str, Callable[[Dict, int], str]])`: Mapping of new column names to generator functions.
        """
        for old_name, new_name in rename_mapping.items():
            self.rename_column(old_name, new_name)

        for column_name, generator in new_columns.items():
            self.add_column(column_name, generator, enum=True)

    @staticmethod
    def _is_valid_number(value: Any) -> bool:
        """
        Check if a value is a valid number.

        Args:
            `value (Any)`: The value to check.

        Returns:
            `bool`: True if the value is a valid number, False otherwise.
        """
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False

    @staticmethod
    def _compute_mean(values: List[float]) -> float:
        """
        Compute the mean of a list of numeric values.

        Args:
            `values (List[float])`: List of numeric values.

        Returns:
            `float`: The mean of the values, or 0.0 if the list is empty.
        """
        return sum(values) / len(values) if values else 0.0

    @staticmethod
    def _compute_median(values: List[float]) -> float:
        """
        Compute the median of a list of numeric values.

        Args:
            `values (List[float])`: List of numeric values.

        Returns:
            `float`: The median of the values, or 0.0 if the list is empty.
        """
        values.sort()
        n = len(values)
        if n == 0:
            return 0.0
        mid = n // 2
        return values[mid] if n % 2 == 1 else (values[mid - 1] + values[mid]) / 2
