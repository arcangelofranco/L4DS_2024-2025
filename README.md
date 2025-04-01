# Group_ID_20 Project

## Description of the Project

This project was developed as part of the course *Decision Support System: Laboratory of Data Science* (Academic Year 2024/2025).  
The main objective is to create a decision support system based on data management techniques and business intelligence. The project is divided into two main parts:

1. **Part 1**: Cleaning, preliminary data analysis, creation of the star schema and population of the data warehouse.
2. **Part 2**: Data cube creation and management, querying using MDX (MultiDimensional Expressions) queries and visualization via interactive dashboard.

---

## Structure of the Project.

### Part 1: **Group_ID_20_Part_1**

- **assignments/**: Scripts and notebooks dedicated to task development.
  - `assignment_1.ipynb`: Notebook for the *Data Understanding*.
  - `assignment_2.py` - `assignment_5.py`: Scripts for the data analysis and transformation steps.

- **data/**: Structure for managing datasets.
  - `cleaned/`: Pre-processed data.
  - `external/`: External data.
  - `raw/`: Raw data collected.
  - `splitted/`: Splited datasets.
  - `group_id_20_db.json`: JSON file for database credentials.

- **modules/**: Reusable Python modules for specific processing.
  - `data.py`: Python Class for data manipulation and transformation.
  - `database.py`: Python Class for handle database connections.
  - `reader.py`: Python Class for reading/exporting data.
  - `utils.py`: Support functions.

- **sql/**: SQL scripts for database schema creation.

- **SSIS/**: Integration packages using SQL Server Integration Services.

- `main.py`: Main script for running data pipelines (`assignment_2.py` - `assignment_5.py`).

---

### Part 2: **Group_ID_20_Part_2**

- **Group_ID_20_Cube/**: Implementation and management of the data cube.
  - `assignments/`: MDX queries used to analyze and query the data cube.
  - `Group_ID_20_Cube.sln`: Solution file for the project.

- **dashboard.pbix**: Interactive dashboard created with Power BI, used to display data and analysis of the data cube.

---

## System Requirements.

Make sure you meet the following prerequisites for running the project:

- **Python**: Version >= 3.8
- **Python libraries**: Listed in `requirements.txt`.
- **SQL Server Management Studio**: For data warehouse management.
- **Power BI Desktop**: For opening and editing the `dashboard.pbix` file.

To install Python dependencies, run:
```bash
pip install -r requirements.txt
```
---

## Contributors
The project was carried out by the **Group ID 20** of the *Decision Support System: Laboratory of Data Science* course at the University of Pisa:
- **Arcangelo Franco**, 584174, *Data Science and Business Informatics*.
- **Nicola Pastorelli**, 656431, *Digital Humanities*

---
