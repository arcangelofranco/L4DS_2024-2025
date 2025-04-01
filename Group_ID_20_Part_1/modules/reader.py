import aiofiles
import csv
from typing import List, Dict, Optional

class Reader:
    """
    A class for reading and exporting `CSV` data.

    Attributes:
        `input_file (str)`: The path to the input file to be loaded.
        `rows (List[Dict[str, str]])`: A list of dictionaries representing the rows of the loaded data.
        `fieldnames (Optional[List[str]])`: A list of field names (columns) in the loaded data.
    """

    def __init__(self) -> None:
        """
        Initializes the Reader object with default attributes.
        """
        self.input_file: str = ""
        self.rows: List[Dict[str, str]] = []
        self.fieldnames: Optional[List[str]] = None

    async def load_data(self, input_file: str) -> None:
        """
        Asynchronously loads data from a `CSV` file.

        Args:
            `input_file (str)`: The path to the `CSV` file to load.

        Raises:
            `FileNotFoundError`: If the file does not exist.
            `ValueError`: If the file content is not valid `CSV`.
        """
        try:
            async with aiofiles.open(input_file, mode="r", encoding="utf-8") as file:
                content = await file.read() 
                reader = csv.DictReader(content.splitlines())
                self.rows = [row for row in reader] 
                self.fieldnames = reader.fieldnames  
        except Exception as e:
            raise Exception(f"Error loading data from file {input_file}: {e}")

    def export_csv(self, output_file: str) -> None:
        """
        Exports the loaded data to a `CSV` file.

        Args:
            `output_file (str)`: The path to the output `CSV` file.

        Raises:
            `ValueError`: If there is no data to export.
            `IOError`: If an error occurs while writing to the file.
        """
        if not self.rows:
            raise ValueError("No data available to export.")
        try:
            with open(output_file, mode="w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=self.fieldnames)
                writer.writeheader()
                writer.writerows(self.rows)
        except IOError as e:
            raise IOError(f"Error writing to file {output_file}: {e}") from e
        except Exception as e:
            raise Exception(f"Unexpected error exporting to file {output_file}: {e}") from e
