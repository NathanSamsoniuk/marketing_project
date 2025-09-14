"""
Calculate marketing KPIs for each record and save to the gold layer.

This script processes cleaned data from the silver layer, calculates key performance
indicators (KPIs) such as CTR, CVR, CPC, CPA, ROAS, and margin for each record, and
saves the results in Parquet and CSV formats in the `data/gold` directory.
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
INPUT_DIR = "data/silver"
OUTPUT_DIR = "data/gold"


def get_latest_silver_file(input_dir: str) -> str:
    """Retrieve the most recent Parquet file from the silver layer using datetime parsing."""
    logger.info(f"Searching for the latest Parquet file in {input_dir}...")
    
    silver_files = [
        f for f in os.listdir(input_dir)
        if f.startswith("marketing") and f.endswith(".parquet")
    ]
    if not silver_files:
        logger.error("No Parquet files found in the silver layer.")
        raise FileNotFoundError("No Parquet files found in the silver layer.")

    def extract_datetime(filename: str) -> datetime:
        try:
            # Remove prefix e extensão
            timestamp_str = filename.replace("marketing_", "").replace(".parquet", "").strip()
            # Tenta converter para datetime
            return datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
        except ValueError:
            # Caso não bata o formato exato, tenta com outros formatos possíveis
            for fmt in ["%Y%m%d_%H%M%S", "%Y%m%d_%H%M"]:
                try:
                    return datetime.strptime(timestamp_str, fmt)
                except ValueError:
                    continue
            logger.error(f"Failed to parse timestamp from filename '{filename}'")
            raise

    # Ordena pelo timestamp extraído, mais recente primeiro
    silver_files.sort(key=extract_datetime, reverse=True)

    latest_file = os.path.join(input_dir, silver_files[0])
    logger.info(f"Latest file found: {latest_file}")
    return latest_file

def calculate_metrics(input_path: str, output_dir: str) -> None:
    """Calculate marketing KPIs for each record and save to the gold layer.

    Args:
        input_path (str): Path to the input Parquet file from the silver layer.
        output_dir (str): Directory to save the processed files.

    Raises:
        FileNotFoundError: If the input file does not exist.
        OSError: If there is an issue saving the output files.
    """
    logger.info(f"Processing data from {input_path}...")

    # Read cleaned data
    try:
        df = pd.read_parquet(input_path)
        logger.info(f"Loaded {len(df)} records from {input_path}")
    except FileNotFoundError as e:
        logger.error(f"Input file not found: {e}")
        raise

    # Calculate KPIs for each record
    df["ctr"] = (df["clicks"] / df["impressions"] * 100).round(2)  # Click-through rate (%)
    df["cvr"] = (df["conversions"] / df["clicks"] * 100).round(2)  # Conversion rate (%)
    df["cpc"] = (df["ad_spend"] / df["clicks"]).round(2)  # Cost per click
    df["cpa"] = (df["ad_spend"] / df["conversions"]).round(2)  # Cost per acquisition
    df["roas"] = (df["revenue"] / df["ad_spend"]).round(2)  # Return on ad spend
    df["margin"] = (
        ((df["revenue"] - df["ad_spend"]) / df["revenue"] * 100)
    ).round(2)  # Profit margin (%)

    # Handle infinite and null values
    for col in ["ctr", "cvr", "cpc", "cpa", "roas", "margin"]:
        df[col] = df[col].replace([float("inf"), -float("inf")], 0).fillna(0)
    logger.info("KPIs calculated and null/infinite values handled.")

    # Add processing timestamp
    df["extraction_date"] = datetime.now()
    logger.info("Added processing timestamp.")

    # Generate output filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_filename = f"marketing_metrics_{timestamp}"

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

    # Process the latest silver file
    latest_file = get_latest_silver_file(INPUT_DIR)
    calculate_metrics(latest_file, OUTPUT_DIR)
