"""
Clean and transform raw marketing data for the silver layer.

This script processes raw data from the bronze layer, performing deduplication,
type standardization, null handling, and validations. The transformed data is saved
in Parquet and CSV formats in the `data/silver` directory.
"""

import logging
import os
from datetime import datetime

import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration constants
INPUT_DIR = "data/bronze"
OUTPUT_DIR = "data/silver"


def get_latest_bronze_file(input_dir: str) -> str:
    """Retrieve the most recent Parquet file from the bronze layer using datetime parsing."""
    logger.info(f"Searching for the latest Parquet file in {input_dir}...")
    
    bronze_files = [
        f for f in os.listdir(input_dir)
        if f.startswith("marketing") and f.endswith(".parquet")
    ]
    if not bronze_files:
        logger.error("No Parquet files found in the bronze layer.")
        raise FileNotFoundError("No Parquet files found in the bronze layer.")

    def extract_datetime(filename: str) -> datetime:
        try:
            timestamp_str = filename.replace("marketing_", "").replace(".parquet", "")
            return datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
        except Exception as e:
            logger.error(f"Failed to parse timestamp from filename '{filename}': {e}")
            raise

    bronze_files.sort(key=extract_datetime, reverse=True)

    latest_file = os.path.join(input_dir, bronze_files[0])
    logger.info(f"Latest file found: {latest_file}")
    return latest_file

def process_silver(input_path: str, output_dir: str) -> None:
    """Clean and transform raw marketing data, saving to the silver layer."""
    logger.info(f"Processing data from {input_path}...")

    # Read raw data
    try:
        df = pd.read_parquet(input_path)
        logger.info(f"Loaded {len(df)} records from {input_path}")
    except FileNotFoundError as e:
        logger.error(f"Input file not found: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to read Parquet file: {e}")
        raise

    # Remove duplicates based on customer_id
    df = df.drop_duplicates(subset=["customer_id"], keep="first")
    logger.info(f"Removed duplicates; {len(df)} records remain.")

    # Standardize data types
    df = df.astype({
        "customer_id": "str",
        "age": "int",
        "gender": "str",
        "income": "float",
        "campaign_id": "str",
        "campaign_channel": "str",
        "campaign_type": "str",
        "ad_spend": "float",
        "impressions": "int",
        "clicks": "int",
        "conversions": "int",
        "website_visits": "int",
        "time_on_site": "int",
        "previous_purchases": "int",
        "advertising_platform": "str",
    })
    df["date_received"] = pd.to_datetime(df["date_received"], errors="coerce")
    logger.info("Data types standardized.")

    # Handle missing values
    df["income"] = df["income"].fillna(df["income"].mean())
    df["ad_spend"] = df["ad_spend"].fillna(0)
    logger.info("Missing values handled.")

    # Add processing timestamp
    df["extraction_date"] = datetime.now()
    logger.info("Added processing timestamp.")

    # Generate output filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_filename = f"marketing_{timestamp}"

    # Save as Parquet
    try:
        parquet_path = os.path.join(output_dir, f"{base_filename}.parquet")
        df.to_parquet(parquet_path, index=False)
        logger.info(f"Data saved to {parquet_path}")
    except OSError as e:
        logger.error(f"Failed to save Parquet file: {e}")
        raise

    # Save as CSV
    try:
        csv_path = os.path.join(output_dir, f"{base_filename}.csv")
        df.to_csv(csv_path, index=False)
        logger.info(f"Data saved to {csv_path}")
    except OSError as e:
        logger.error(f"Failed to save CSV file: {e}")
        raise


if __name__ == "__main__":
    # Ensure output directory exists
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        logger.info(f"Output directory {OUTPUT_DIR} ensured.")
    except OSError as e:
        logger.error(f"Failed to create output directory {OUTPUT_DIR}: {e}")
        raise

    # Process the latest bronze file
    latest_file = get_latest_bronze_file(INPUT_DIR)
    process_silver(latest_file, OUTPUT_DIR)
