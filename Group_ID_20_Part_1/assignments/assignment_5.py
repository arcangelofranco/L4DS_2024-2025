import os
import sys
import asyncio
import logging as log
from modules.utils import (
    get_root, get_paths, read_json, log_execution
)
from modules.data import Data
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
async def populate_database():
    """
    Populates the database with data from pre-processed datasets.

    This function:
    1. Retrieves the root path of the project and adds it to the system path.
    2. Loads paths to split datasets (exported earlier).
    3. Initializes the datasets for each table in the database.
    4. Reads the database credentials from a `JSON` file.
    5. Connects to the database using the provided credentials.
    6. Inserts data into corresponding database tables.
    7. Handles errors and ensures the database connection is properly closed.

    Raises:
        `Exception`: If there is an error during database population.
    """
    root_path = get_root("dss")
    sys.path.append(root_path)

    data_paths = get_paths(os.path.join(root_path, "Group_ID_20_Part_1"), "splitted")

    datasets = {
        "CRASH": Data(data_paths["CRASH"]),
        "DATE": Data(data_paths["DATE"]),
        "LOCATION": Data(data_paths["LOCATION"]),
        "INJURY": Data(data_paths["INJURY"]),
        "PERSON": Data(data_paths["PERSON"]),
        "VEHICLE": Data(data_paths["VEHICLE"]),
        "DAMAGE": Data(data_paths["DAMAGE"]),
    }

    await asyncio.gather(*(dataset.initialize() for dataset in datasets.values()))

    credentials_path = os.path.join(root_path, "Group_ID_20_Part_1", "data", "group_id_20_db.json")
    credentials = read_json(credentials_path)

    db = Database(
        server=credentials["server"],
        db=credentials["db"],
        user=credentials["user"],
        pwd=credentials["pwd"]
    )
    
    try:
        await db.connect()

        for dataset_key, table_name in [
            ("CRASH", "crash"),
            ("DATE", "date"),
            ("LOCATION", "location"),
            ("INJURY", "injury"),
            ("PERSON", "person"),
            ("VEHICLE", "vehicle"),
        ]:
            await db.data_to_db(datasets[dataset_key], table_name)
            
        await db.data_to_db(datasets["DAMAGE"], "damage")

    except Exception as e:
        raise Exception(f"Error during database population: {e}")
    
    finally:
        await db.disconnect()


if __name__ == "__main__":
    asyncio.run(populate_database())
