# Marketing Data Pipeline

## Overview
This project implements a data engineering pipeline for processing marketing data across three distinct layers: **Bronze**, **Silver**, and **Gold**.  

The pipeline generates synthetic marketing data, applies data cleaning and transformation, and calculates key performance indicators (KPIs) for analysis. It is designed with **modularity, scalability, and maintainability** in mind. All data is stored in both **Parquet** and **CSV** formats to balance efficiency and accessibility.

## Project Structure

data/ # Stores data at different processing stages

├── bronze/ # Raw synthetic data (Parquet & CSV)

├── silver/ # Cleaned and transformed data (Parquet & CSV)

└── gold/ # Aggregated KPI data (Parquet & CSV)

src/ # Python scripts for data processing

├── bronze/ # Raw data generation

├── silver/ # Data cleaning and transformation

└── gold/ # KPI calculation

.gitignore # Files/directories excluded from version control
.python-version # Python version (managed with pyenv)
requirements.txt # Python dependencies


### Data Layers
- **Bronze**: Raw synthetic data files (e.g., `marketing_<timestamp>.parquet`, `marketing_<timestamp>.csv`).  
- **Silver**: Cleaned and transformed datasets.  
- **Gold**: Aggregated KPI datasets (e.g., `marketing_metrics_<timestamp>.parquet`, `marketing_metrics_<timestamp>.csv`).  

### Scripts
- **Bronze Layer**  
  `generate_raw_marketing_data.py` → Generates synthetic marketing data using the *Faker* library.  
- **Silver Layer**  
  `clean_transform_marketing.py` → Cleans raw data, removes duplicates, standardizes data types, and validates records.  
- **Gold Layer**  
  `calculate_kpi_marketing.py` → Calculates KPIs such as CTR, CVR, CPC, CPA, ROAS, and Margin.  

## Pipeline Workflow
1. **Bronze Layer**  
   - Generates synthetic marketing data including customer details, campaign metrics, and timestamps.  
   - Stores files in `data/bronze/`.  

2. **Silver Layer**  
   - Reads the latest Bronze dataset.  
   - Removes duplicates, standardizes data types, handles missing values, and applies validations.  
   - Stores results in `data/silver/`.  

3. **Gold Layer**  
   - Reads the latest Silver dataset.  
   - Calculates key marketing KPIs.  
   - Stores results in `data/gold/`.  

## Prerequisites
- **pyenv** installed for Python version management.  
- Python version aligned with `.python-version`.  
- A virtual environment with required dependencies from `requirements.txt`.  

## How to Run

1. **Set the Python version** (based on `.python-version`)

   - If you have `pyenv` installed:
     ```bash
     # Option A: set local version by reading the file (Unix shells)
     pyenv local $(cat .python-version)

     # Option B: open .python-version and run with the exact version shown
     pyenv local <python-version>
     ```
     *If the version is not installed yet, run `pyenv install <python-version>` first.*

   - Verify Python version:
     ```bash
     python --version
     ```

2. **Create and activate a virtual environment** (if you don't already have one)
   ```bash
   # Create venv
   python -m venv venv

   # Activate (Linux / macOS)
   source venv/bin/activate

   # Activate (Windows - CMD)
   venv\Scripts\activate

   # Activate (Windows - PowerShell)
   venv\Scripts\Activate.ps1
3. **Install dependencies**
    ```bash
    pip install -r requirements.txt
    ```

4. **Run the pipeline scripts in sequence**
    ```bash
    python src/bronze/generate_raw_marketing_data.py
    python src/silver/clean_transform_marketing.py
    python src/gold/calculate_kpi_marketing.py
    ```
  


Output

Bronze: Raw data containing customer and campaign details.

Silver: Cleaned and validated datasets with standardized formats.

Gold: KPI datasets including:
  - CTR (Click-Through Rate)
  - CVR (Conversion Rate)
  - CPC (Cost per Click)
  - CPA (Cost per Acquisition)
  - ROAS (Return on Ad Spend)
  - Margin
