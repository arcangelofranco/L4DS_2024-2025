import asyncio
from modules.utils import log_execution
from assignments.assignment_2 import process_data
from assignments.assignment_3 import create_schema
from assignments.assignment_4 import generate_starschema_files
from assignments.assignment_5 import populate_database

@log_execution
async def main() -> None:
    """
        Main asynchronous function that orchestrates the execution of multiple data processing tasks.

        This function performs the following tasks in sequence:
        1. Processes the data using the `process_data` function.
        2. Creates the database schema using the `create_schema` function.
        3. Initializes datasets and generate star schema files using the `generate_starschema_files` function.
        4. Populates the database with data using the `populate_database` function.
    """
    # await process_data()
    # await create_schema()
    await generate_starschema_files()
    # await populate_database()


if __name__ == "__main__":
    asyncio.run(main())