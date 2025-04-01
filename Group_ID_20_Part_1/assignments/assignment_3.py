import os
import sys
import asyncio
import logging as log

from modules.utils import (
    get_root, read_json, log_execution
)
from modules.database import Database


log.basicConfig(
    level=log.DEBUG,
    format="%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s",
    handlers=[
        log.StreamHandler()
    ]
)
log.getLogger("asyncio").setLevel(log.WARNING)

@log_execution
async def create_schema():
    """
    Creates the database schema by reading credentials and `SQL` query from files,
    connecting to the database, and executing the `SQL` query to create the schema.

    This function:
    1. Reads credentials from a `JSON` file.
    2. Creates a Database instance and connects to the database.
    3. Reads the `SQL` schema file.
    4. Executes the `SQL` query to create the database schema.
    5. Handles errors and ensures proper disconnection from the database.

    Raises:
        `Exception`: If an error occurs during the schema creation process.
    """
    root_path = get_root("dss")
    
    sys.path.append(root_path)

    credentials_path = os.path.join(root_path, "Group_ID_20_Part_1", "data", "group_id_20_db.json")
    sql_file_path = os.path.join(root_path, "Group_ID_20_Part_1", "sql", "schema.sql")
    
    credentials = read_json(credentials_path)
    
    db = Database(
        server=credentials["server"],
        db=credentials["db"],
        user=credentials["user"],
        pwd=credentials["pwd"]
    )
    
    try:
        await db.connect()
        
        sql_query = await db.read_sql_file(sql_file_path)
        
        await db.execute_query(sql_query)
    except Exception as e:
        raise Exception(f"Error during schema creation: {e}")
    finally:
        await db.disconnect()

if __name__ == "__main__":
    asyncio.run(create_schema())