import sys
import os
import asyncio
import logging as log
from typing import Dict, List
from modules.utils import (
    get_root, get_paths, log_execution
)
from modules.data import Data

log.basicConfig(
    level=log.DEBUG,
    format="%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s",
    handlers=[
        log.StreamHandler()
    ]
)

log.getLogger("asyncio").setLevel(log.WARNING)

# Definition of columns for each table (schema) in the dataset.
SCHEMA_DEFINITIONS = {
    "CRASH": ["CRASH_ID", "RD_NO", "CRASH_DATE", "POSTED_SPEED_LIMIT", "TRAFFIC_CONTROL_DEVICE", 
              "DEVICE_CONDITION", "WEATHER_CONDITION", "LIGHTING_CONDITION", "FIRST_CRASH_TYPE", 
              "TRAFFICWAY_TYPE", "ALIGNMENT", "ROADWAY_SURFACE_COND", "ROAD_DEFECT", "REPORT_TYPE", 
              "CRASH_TYPE", "PRIM_CONTRIBUTORY_CAUSE", "SEC_CONTRIBUTORY_CAUSE"],
    "DATE": ["DATE_ID", "CRASH_TIME", "CRASH_PERIOD", "CRASH_DAY", "CRASH_MONTH", "CRASH_YEAR", "CRASH_DAY_OF_WEEK", 
            "CRASH_SEASON", "DATE_POLICE_NOTIFIED"],
    "LOCATION": ["LOCATION_ID", "STREET_NO", "STREET_DIRECTION", "STREET_NAME", 
                 "BEAT_OF_OCCURRENCE", "LATITUDE", "LONGITUDE", "LOCATION_POINT"],
    "INJURY": ["INJURY_ID", "MOST_SEVERE_INJURY", "INJURIES_TOTAL", "INJURIES_FATAL", 
               "INJURIES_INCAPACITATING", "INJURIES_NON_INCAPACITATING", 
               "INJURIES_REPORTED_NOT_EVIDENT", "INJURIES_NO_INDICATION"],
    "PERSON": ["PERSON_ID", "PERSON_TYPE", "CRASH_DATE", "CITY", "STATE", "SEX", "AGE", 
               "SAFETY_EQUIPMENT", "AIRBAG_DEPLOYED", "EJECTION", "INJURY_CLASSIFICATION", 
               "DRIVER_ACTION", "DRIVER_VISION", "PHYSICAL_CONDITION", "BAC_RESULT", 
               "DAMAGE_CATEGORY"],
    "VEHICLE": ["VEHICLE_ID", "CRASH_DATE", "UNIT_NO", "UNIT_TYPE", "MAKE", "MODEL", 
                "LIC_PLATE_STATE", "VEHICLE_YEAR", "VEHICLE_DEFECT", "VEHICLE_TYPE", 
                "VEHICLE_USE", "TRAVEL_DIRECTION", "MANEUVER", "OCCUPANT_CNT", 
                "FIRST_CONTACT_POINT"],
    "DAMAGE": ["DAMAGE_COST", "NUM_UNITS", "CRASH_ID", "DATE_ID", "LOCATION_ID", 
               "INJURY_ID", "PERSON_ID", "VEHICLE_ID"]
}


def join_data(obj1: Data, obj2: Data, column: str) -> "Data":
    """
    Merges two datasets based on a common column.

    Args:
        `obj1 (Data)`: The first dataset.
        `obj2 (Data)`: The second dataset.
        `column (str)`: The column name to join the datasets on.

    Returns:
        `Data`: A new Data object containing the merged data.

    Raises:
        `KeyError`: If the specified column does not exist in both datasets.
    """
    for data in [obj1, obj2]:
        if column not in data.fieldnames:
            raise KeyError(f"Column '{column}' is not present in dataset {data}")

    obj1_dict = {row[column]: row for row in obj1.rows}

    headers1 = obj1.fieldnames
    headers2 = obj2.fieldnames
    combined_headers = headers1 + [h for h in headers2 if h != column]

    joined_data = []
    for row in obj2.rows:
        key = row[column]
        if key in obj1_dict:
            joined_row = {**obj1_dict[key], **{k: v for k, v in row.items() if k != column}}
            joined_data.append(joined_row)

    merged_data = Data.__new__(Data)
    merged_data.rows = joined_data
    merged_data.fieldnames = combined_headers

    return merged_data

@log_execution
async def export_data(obj: Data, columns: List[str], export_path: str) -> None:
    """
    Exports filtered data to a CSV file with selected columns.

    Args:
        `obj (Data)`: The dataset to export.
        `columns (List[str])`: The list of columns to include in the exported data.
        `export_path (str)`: The file path where the data will be exported.
    """
    filtered_data = obj.copy()
    filtered_data.update_columns(columns)

    ordered_rows = [
        {col: row.get(col, None) for col in columns} for row in filtered_data.rows
    ]

    filtered_data.rows = ordered_rows
    filtered_data.fieldnames = columns

    filtered_data.export_csv(export_path)

@log_execution
async def split_and_export_schemas(datasets: Dict[str, Data], root_path: str) -> None:
    """
    Splits and exports datasets according to predefined schema definitions.

    Args:
        `datasets (Dict[str, Data])`: A dictionary of datasets to process.
        `root_path (str)`: The root path of the project, used for defining export directory.
    """
    export_dir = os.path.join(root_path, "Group_ID_20_Part_1", "data", "splitted")
    os.makedirs(export_dir, exist_ok=True)

    for schema_name, columns in SCHEMA_DEFINITIONS.items():
        source_dataset = "CRASHES" if schema_name in ["CRASH", "DATE", "LOCATION", "INJURY"] else \
                        "PEOPLE" if schema_name == "PERSON" else \
                        "VEHICLES" if schema_name == "VEHICLE" else None

        if schema_name == "DAMAGE":
            crashes_people = join_data(datasets["CRASHES"], datasets["PEOPLE"], "RD_NO")
            merged_data = join_data(datasets["VEHICLES"], crashes_people, "RD_NO")
            await export_data(merged_data, columns, os.path.join(export_dir, f"{schema_name.lower()}.csv"))
        else:
            dataset = datasets[source_dataset]
            await export_data(dataset, columns, os.path.join(export_dir, f"{schema_name.lower()}.csv"))

@log_execution
async def generate_starschema_files() -> None:
    """
    Initializes datasets and initiates the export process for split schemas.

    This function:
    1. Retrieves and processes the dataset paths.
    2. Initializes the datasets.
    3. Splits the datasets according to predefined schema definitions and exports them as `CSV` files.
    """
    root_path = get_root("dss")
    sys.path.append(root_path)

    data_paths = get_paths(os.path.join(root_path, "Group_ID_20_Part_1"), "cleaned")

    datasets =  {
        "CRASHES": Data(data_paths["CRASHES"]),
        "PEOPLE": Data(data_paths["PEOPLE"]),
        "VEHICLES": Data(data_paths["VEHICLES"]),
    }

    await asyncio.gather(*(dataset.initialize() for dataset in datasets.values()))

    try:
        await split_and_export_schemas(datasets, root_path)
    except Exception as e:
        raise Exception(f"Error during execution: {e}")


if __name__ == "__main__":
    asyncio.run(generate_starschema_files())
