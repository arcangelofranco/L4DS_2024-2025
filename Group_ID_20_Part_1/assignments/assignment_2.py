import os
import sys
import logging as log
import asyncio

from modules.data import Data
from modules.utils import (
    get_root, get_paths, remove_brackets_and_following, 
    remove_after_symbols, remove_irrelevants, remove_quotes, handle_punctation
)
from modules.utils import log_execution
from shapely.wkt import loads
from typing import List, Dict, Optional, Tuple

log.basicConfig(
    level=log.DEBUG,
    format="%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s",
    handlers=[
        log.StreamHandler()
    ]
)

log.getLogger("asyncio").setLevel(log.WARNING)  

def classify_injury(row: Dict[str, str]) -> Optional[str]:
    """
    Classifies the injury based on the severity levels in the given row.

    Args:
        `row (Dict[str, str])`: A dictionary representing a single row of accident data.

    Returns:
        `Optional[str]`: The injury classification based on the highest injury value or None if unknown.
    """
    injury_mapping = {
        'INJURIES_FATAL': 'FATAL',
        'INJURIES_INCAPACITATING': 'INCAPACITATING INJURY',
        'INJURIES_NON_INCAPACITATING': 'NON-INCAPACITATING INJURY',
        'INJURIES_REPORTED_NOT_EVIDENT': 'REPORTED NOT EVIDENT',
        'INJURIES_NO_INDICATION': 'NO INDICATION OF INJURY',
    }
    return max(
        injury_mapping.items(),
        key=lambda col_label: float(row.get(col_label[0], 0) or 0)
    )[1]

def apply_replacements(obj: Data, replacements: List[Tuple]) -> None:
    """
    Applies specified replacements to columns in the provided data object.

    Args:
        `obj (Data)`: The Data object containing the dataset to be modified.
        `replacements (List[Tuple])`: A list of tuples where each tuple contains:
            - column name (str),
            - a condition (Callable), 
            - new value to replace (Any).
    """
    for column, condition, new_value in replacements:
        obj.replace_column_values(column, condition, new_value)

def cast_columns(obj: Data, columns: List[str], cast_type: type) -> None:
    """
    Casts specified columns in the Data object to the given type.

    Args:
        `obj (Data)`: The Data object containing the dataset.
        `columns (List[str])`: A list of column names to be cast.
        `cast_type (type)`: The type to cast the columns to (e.g., int, float).
    """
    for column in columns:
        obj.cast_column(column, cast_type)

@log_execution
async def process_crashes(obj: Data, beats: Data) -> None:
    """
    Processes the crash data by applying replacements, filtering rows, 
    handling missing values, and enhancing data with new calculated fields.

    Args:
        `obj (Data)`: The Data object containing the crash data.
        `beats (Data)`: The Data object containing the police beat data for geographic information.
    """
    beats_map = {
        beat["BEAT_NUM"].lstrip('0'): loads(beat["the_geom"])  
        for beat in beats.rows
    }

    day_of_week_mapping = {
        str(i): day for i, day in enumerate(
            ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"], 1
        )
    }

    replacements = [
        ("POSTED_SPEED_LIMIT", lambda x: x.isdigit() and int(x) > 70, 70),
        ("POSTED_SPEED_LIMIT", lambda x: x.isdigit() and int(x) < 20, 20),
        ("REPORT_TYPE", lambda x: not x, "unknown".upper()),
        ("STREET_NAME", lambda x: not x, "unkwnon".upper()),
        ("STREET_DIRECTION", lambda x: not x, "U"),
        ("MOST_SEVERE_INJURY", lambda x: not x, "NO INDICATION OF INJURY"),
        ("CRASH_DAY_OF_WEEK", lambda x: str(x).strip() in day_of_week_mapping, lambda x: day_of_week_mapping[str(x).strip()])
    ]
    
    apply_replacements(obj, replacements)

    obj.filter_rows("BEAT_OF_OCCURRENCE")
    obj.remove_columns(["CRASH_HOUR", "CRASH_MONTH", "INJURIES_UNKNOWN"])  
    
    obj.split_datetime("CRASH_DATE")
    
    for row in obj.rows:
        if row.get("LONGITUDE") == '' and row.get("LATITUDE") == '':
            beat_num = str(int(float(row["BEAT_OF_OCCURRENCE"]))).lstrip('0')
            if beat_num in beats_map:
                centroids = beats_map[beat_num].centroid
                row["LATITUDE"], row["LONGITUDE"] = round(centroids.y, 6), round(centroids.x, 6)

        row["LOCATION"] = obj.get_geohash(float(row["LATITUDE"]), float(row["LONGITUDE"]))

        for field in [
            "TRAFFICWAY_TYPE", "PRIM_CONTRIBUTORY_CAUSE", "SEC_CONTRIBUTORY_CAUSE", "ROAD_DEFECT",
            "LIGHTING_CONDITION", "FIRST_CRASH_TYPE", "MOST_SEVERE_INJURY", "ALIGNMENT", "ROADWAY_SURFACE_COND"
        ]:
            row[field] = handle_punctation(row[field], replace=True, placer=";")
            row[field] = remove_quotes(row[field])

        for field in ["TRAFFICWAY_TYPE", "REPORT_TYPE", "PRIM_CONTRIBUTORY_CAUSE", "SEC_CONTRIBUTORY_CAUSE"]:
            row[field] = remove_brackets_and_following(row[field])

    cast_columns(obj, [
        "BEAT_OF_OCCURRENCE", "NUM_UNITS", "INJURIES_TOTAL", "INJURIES_FATAL",
        "INJURIES_INCAPACITATING", "INJURIES_NON_INCAPACITATING", 
        "INJURIES_REPORTED_NOT_EVIDENT", "INJURIES_NO_INDICATION"
    ], int)

    obj.enhance_data(
        rename_mapping={"LOCATION": "LOCATION_POINT"},
        new_columns={
            "CRASH_ID": lambda _, idx: f"CRS_{idx + 1:06d}",
            "DATE_ID": lambda _, idx: f"DT_{idx + 1:06d}",
            "LOCATION_ID": lambda _, idx: f"LCT_{idx + 1:06d}",
            "INJURY_ID": lambda _, idx: f"NJR_{idx + 1:06d}",
        }
    )

@log_execution
async def process_people(obj: Data, crashes: Data, city: str) -> None:
    """
    Processes the people data by applying replacements, assigning injury classifications,
    and handling various data fields.

    Args:
        `obj (Data)`: The Data object containing the people data.
        `crashes (Data)`: The Data object containing the crash data.
        `city (str)`: The name of the city for city-state correction.
    """
    crash_case = {row["RD_NO"]: row for row in crashes.rows}
    await obj.load_city_state(city)

    replacements = [
        ("VEHICLE_ID", lambda x: not x, 0),
        ("CITY", lambda x: not x, "unknown".upper()),
        ("STATE", lambda x: not x, "XX"),
        ("SEX", lambda x: not x, "X"),
        ("SEX", lambda x: x == "U", "X"),
        ("AGE", lambda x: not x, obj.replace_central_tendency("AGE")),
        ("SAFETY_EQUIPMENT", lambda x: not x, "unknown".upper()),
        ("AIRBAG_DEPLOYED", lambda x: not x, "unknown".upper()),
        ("EJECTION", lambda x: not x, "unknown".upper()),
        ("DRIVER_ACTION", lambda x: not x, "unknown".upper()),
        ("DRIVER_VISION", lambda x: not x, "unknown".upper()),
        ("PHYSICAL_CONDITION", lambda x: not x, "unknown".upper()),
        ("BAC_RESULT", lambda x: not x, "TEST NOT OFFERED"),
    ]

    apply_replacements(obj, replacements)
    
    for row in obj.rows:
        crash_case_id = row.get("RD_NO")
        if crash_case_id in crash_case:
            crash_row = crash_case[crash_case_id]
            row["INJURY_CLASSIFICATION"] = classify_injury(crash_row)

    for row in obj.rows:
        if row["CITY"] != "unknown".upper():
            row["CITY"], row["STATE"] = obj.correct_city(row["CITY"])

        if row.get("DAMAGE_CATEGORY") == "$500 OR LESS" and not row.get("DAMAGE"):
            row["DAMAGE"] = 250.0

        for field in ["AIRBAG_DEPLOYED", "DRIVER_VISION", "DAMAGE_CATEGORY", "BAC_RESULT"]:
            if field == "DRIVER_VISION":
                row[field] = handle_punctation(row[field], replace=True, placer=" OR")
            else:
                row[field] = handle_punctation(row[field])

        for field in ["CITY", "AIRBAG_DEPLOYED", "DRIVER_VISION"]:
            row[field] = remove_brackets_and_following(row[field])

    cast_columns(obj, ["AGE", "VEHICLE_ID"], int)

    obj.enhance_data(
        rename_mapping={"PERSON_ID": "PERSON", "VEHICLE_ID": "VEHICLE", "DAMAGE": "DAMAGE_COST"},
        new_columns={"PERSON_ID": lambda _, idx: f"PRS_{idx + 1:06d}"}
    )

@log_execution
async def process_vehicles(obj: Data) -> None:
    """
    Processes the vehicle data by applying replacements, cleaning fields, and enhancing data.

    Args:
        `obj (Data)`: The Data object containing the vehicle data.
    """
    replacements = [
        ("VEHICLE_ID", lambda x: not x, 0),
        ("MAKE", lambda x: not x, "unknown".upper()),
        ("MODEL", lambda x: not x, "unknown".upper()),
        ("LIC_PLATE_STATE", lambda x: not x, "XX"),
        ("VEHICLE_YEAR", lambda x: not x, obj.replace_central_tendency("VEHICLE_YEAR")),
        ("VEHICLE_YEAR", lambda x: isinstance(x, str) and float(x) < 1886, 1886.0),
        ("VEHICLE_YEAR", lambda x: isinstance(x, str) and float(x) > 2018, 2018.0),
        ("VEHICLE_DEFECT", lambda x: not x, "unknown".upper()),
        ("VEHICLE_USE", lambda x: not x, "unknown".upper()),
        ("TRAVEL_DIRECTION", lambda x: not x, "U"),
        ("FIRST_CONTACT_POINT", lambda x: not x, "unknown".upper()),
        ("OCCUPANT_CNT", lambda x: not x, obj.replace_central_tendency("OCCUPANT_CNT")),
        ("MANEUVER", lambda x: not x, "unknown".upper()),
        ("UNIT_TYPE", lambda x: not x, "unknown".upper()),
        ("VEHICLE_TYPE", lambda x: not x, "unknown".upper()),
    ]

    apply_replacements(obj, replacements)

    for row in obj.rows:
        for field in row:
            if row[field] == "UNKNOWN/NA":
                row[field] = "unknown".upper()

        for field in ["MAKE", "MODEL"]:
            row[field] = remove_brackets_and_following(row[field])
            row[field] = remove_after_symbols(row[field]) 
            row[field] = remove_irrelevants(row[field])
            row[field] = remove_quotes(row[field])
            
            if not row[field].strip():
                row[field] = "unknown".upper()

        for field in ["VEHICLE_TYPE", "FIRST_CONTACT_POINT"]:
            row[field] = remove_brackets_and_following(row[field])

    cast_columns(obj, ["VEHICLE_YEAR", "OCCUPANT_CNT", "VEHICLE_ID"], int)

    obj.enhance_data(
        rename_mapping={"VEHICLE_ID": "VEHICLE"},
        new_columns={"VEHICLE_ID": lambda _, idx: f"VHC_{idx + 1:06d}"}
    )

@log_execution
async def process_data() -> None:
    """
    Main asynchronous function that processes and cleans the datasets of crashes, people, and vehicles.

    This function:
    1. Loads raw data for crashes, people, and vehicles.
    2. Applies the necessary processing functions to clean the data.
    3. Exports the cleaned datasets to `CSV` files.

    Raises:
        Exception: If any errors occur during data processing.
    """

    root_path = get_root("dss")
    sys.path.append(root_path)

    data_paths = get_paths(os.path.join(root_path, "Group_ID_20_Part_1"), "raw")

    datasets = {
        "CRASHES": Data(data_paths["CRASHES"]),
        "PEOPLE": Data(data_paths["PEOPLE"]),
        "VEHICLES": Data(data_paths["VEHICLES"]),
        "POLICE_BEAT": Data(data_paths["POLICE_BEAT"]),
    }

    await asyncio.gather(*(dataset.initialize() for dataset in datasets.values()))
    
    try:
        await process_crashes(datasets["CRASHES"], datasets["POLICE_BEAT"])
        datasets["CRASHES"].export_csv(os.path.join(root_path, "Group_ID_20_Part_1", "data", "cleaned", "crashes_cleaned.csv"))

        await process_people(datasets["PEOPLE"], datasets["CRASHES"], data_paths["CITY_US"])
        datasets["PEOPLE"].export_csv(os.path.join(root_path, "Group_ID_20_Part_1", "data", "cleaned", "people_cleaned.csv"))

        await process_vehicles(datasets["VEHICLES"])
        datasets["VEHICLES"].export_csv(os.path.join(root_path, "Group_ID_20_Part_1", "data", "cleaned", "vehicles_cleaned.csv"))

    except Exception as ex:
        log.error(f"An error occurred: {ex}")
        raise

if __name__ == "__main__":
    asyncio.run(process_data())