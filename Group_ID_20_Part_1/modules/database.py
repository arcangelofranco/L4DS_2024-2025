import os
import aioodbc
import aiofiles
from typing import Any
from modules.data import Data

class Database:
    """
    A class for handling asynchronous database operations.

    Attributes:
        `server (str)`: The database server address.
        `db (str)`: The database name.
        `user (str)`: The username for database authentication.
        `pwd (str)`: The password for database authentication.
        `connection`: The active database connection, or None if not connected.
    """

    def __init__(self, server: str, db: str, user: str, pwd: str) -> None:
        """
        Initializes the Database object with connection parameters.

        Args:
            `server (str)`: The database server address.
            `db (str)`: The database name.
            `user (str)`: The username for authentication.
            `pwd (str)`: The password for authentication.
        """
        self.server = server
        self.db = db 
        self.user = user 
        self.pwd = pwd
        self.connection = None

    async def connect(self):
        """
        Establishes a connection to the database.

        Raises:
            `ConnectionError`: If the connection fails.
        """
        try:
            self.connection = await aioodbc.connect(
                dsn=f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={self.server};"
                    f"DATABASE={self.db};"
                    f"UID={self.user};"
                    f"PWD={self.pwd}"
            )
        except Exception as e:
            raise ConnectionError(f"Connection Error: {e}")
        
    async def disconnect(self):
        """
        Closes the database connection if it is open.
        """
        if self.connection:
            await self.connection.close()
            self.connection = None

    async def execute_query(self, query: str, params: str = None):
        """
        Executes a `SQL` query on the database.

        Args:
            `query (str)`: The SQL query to execute.
            `params (str, optional)`: Parameters for the query.

        Raises:
            `ConnectionError`: If the database is not connected.
            `Exception`: If an error occurs during query execution.
        """
        if not self.connection:
            raise ConnectionError("Database is not connected.")
        try:
            async with self.connection.cursor() as cursor:
                if params:
                    await cursor.execute(query, params)
                else: 
                    await cursor.execute(query)
                await self.connection.commit()
        except Exception as e:
            raise Exception(f"Query execution error: {e}")
        
    async def fetch_query(self, query: str) -> Any:
        """
        Executes a `SQL` query and `fetches` the results.

        Args:
            `query (str)`: The SQL query to execute.

        Returns:
            `Any`: The results of the query.

        Raises:
            `ConnectionError`: If the database is not connected.
            `Exception`: If an error occurs during query execution.
        """
        if not self.connection:
            raise ConnectionError("Database is not connected.")
        try:
            async with self.connection.cursor() as cursor:
                await cursor.execute(query)
                return await cursor.fetchall()
        except Exception as e:
            raise Exception(f"Query execution error: {e}")
        
    async def data_to_db(self, data: Data, table_name: str, batch_size: int = 10000):
        """
        Inserts data from a `Data` object into a database table in batches.

        Args:
            `data (Data)`: The data to insert.
            `table_name (str)`: The name of the target database table.
            `batch_size (int, optional)`: The size of each batch of rows to insert. Defaults to 10,000.

        Raises:
            `ConnectionError`: If the database is not connected.
            `ValueError`: If the `Data` object is empty or improperly initialized.
            `Exception`: If an error occurs during data insertion.
        """
        if not self.connection:
            raise ConnectionError("Database is not connected.")
        if not data.rows or not data.fieldnames:
            raise ValueError("Data object is empty or improperly initialized.")

        try:
            placeholders = ', '.join(['?'] * len(data.fieldnames))
            query = f"INSERT INTO {table_name} ({', '.join(data.fieldnames)}) VALUES ({placeholders})"

            async with self.connection.cursor() as cursor:
                rows = [tuple(row[field] for field in data.fieldnames) for row in data.rows]
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i + batch_size]
                    await cursor.executemany(query, batch)
                await self.connection.commit()
        except Exception as e:
            raise Exception(f"Error during data insertion: {e}")

    @staticmethod
    async def read_sql_file(file_path: str) -> str:
        """
        Reads the contents of an `SQL` file asynchronously.

        Args:
            `file_path (str)`: The path to the `SQL` file.

        Returns:
            `str`: The contents of the `SQL` file.

        Raises:
            `FileNotFoundError`: If the file does not exist.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"SQL file {file_path} does not exist.")
        async with aiofiles.open(file_path, 'r') as file:
            return await file.read()
