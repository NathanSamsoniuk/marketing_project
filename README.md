Marketing Data Pipeline

Overview

This project implements a data engineering pipeline for processing marketing data through three distinct layers: bronze, silver, and gold. It generates synthetic marketing data, performs cleaning and transformation, and calculates key performance indicators (KPIs) for analysis. The pipeline is designed for modularity, scalability, and maintainability, with data stored in both Parquet and CSV formats for flexibility and accessibility.

Project Structure

data/: Stores data at different processing stages:

bronze/: Contains raw synthetic data in Parquet and CSV formats (e.g., marketing_<timestamp>.parquet, marketing_<timestamp>.csv).

silver/: Contains cleaned and transformed data in Parquet and CSV formats.

gold/: Contains aggregated data with KPIs in Parquet and CSV formats (e.g., marketing_metrics_<timestamp>.parquet, marketing_metrics_<timestamp>.csv).


src/: Contains Python scripts for data processing, organized by layer:

bronze/: generate_raw_marketing_data.py generates synthetic marketing data using the Faker library.

silver/: clean_transform_marketing.py cleans raw data, removes duplicates, standardizes data types, and validates data.

gold/: aggregate_kpi_marketing.py calculates KPIs such as CTR, CVR, CPC, CPA, ROAS, and margin.


.gitignore: Specifies files and directories to exclude from version control (e.g., virtual environments, temporary files).

.python-version: Specifies the Python version for the project, managed by pyenv.

requirements.txt: Lists Python dependencies required for the project (e.g., pandas, faker, pyarrow).

Pipeline Workflow

Bronze Layer: The generate_raw_marketing_data.py script creates synthetic marketing data, including customer details, campaign metrics, and timestamps. Data is saved in data/bronze.

Silver Layer: The clean_transform_marketing.py script reads the latest bronze data, removes duplicates, standardizes data types, handles missing values, and performs validations. Output is saved in data/silver.

Gold Layer: The aggregate_kpi_marketing.py script reads the latest silver data, calculates KPIs for each record, and saves the results in data/gold.

Prerequisites

pyenv installed to manage Python versions

Python version matching the one specified in .python-version

A virtual environment with dependencies installed (listed in requirements.txt)

How to Run

Set the Python version using pyenv based on .python-version:pyenv local


Verify the Python version:python --version


Activate the virtual environment:source venv/bin/activate  # Linux/Mac

venv\Scripts\activate     # Windows


Run the scripts in sequence:

python src/bronze/generate_raw_marketing_data.py
python src/silver/clean_transform_marketing.py
python src/gold/aggregate_kpi_marketing.py



Output

Bronze: Raw data files in data/bronze containing customer and campaign details.

Silver: Cleaned and validated data files in data/silver with standardized formats.

Gold: KPI data files in data/gold with metrics such as CTR (Click-Through Rate), CVR (Conversion Rate), CPC (Cost Per Click), CPA (Cost Per Acquisition), ROAS (Return on Ad Spend), and margin.

Notes

The pyenv local command ensures the Python version matches .python-version for compatibility.

Each script processes the latest Parquet file in its respective input directory based on the timestamp in the filename.

Comprehensive logging is implemented to facilitate debugging and monitoring.

Data is saved in both Parquet (optimized for storage and processing) and CSV (human-readable) formats.

The pipeline is designed to run sequentially, with each script depending on the output of the previous layer.
