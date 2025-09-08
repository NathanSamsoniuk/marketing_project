"""
Generate synthetic raw marketing data and save it to the bronze layer.

This script creates synthetic marketing data using Faker, including customer details,
campaign metrics, and timestamps. The data is saved in Parquet and CSV formats in the
`data/bronze` directory.
"""

import logging
import os
from datetime import datetime
import random

import numpy as np
import pandas as pd
from faker import Faker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize Faker for synthetic data generation
fake = Faker()

# Configuration constants
NUM_CUSTOMERS = [5000]  # Number of customers to generate
OUTPUT_DIR = "data/bronze"
CAMPAIGN_IDS = [
    "92c14ef8-a59a-4a78-9670-9e527d9947a1",
    "4c967788-de7b-4626-bbca-c7e7144da864",
]
CAMPAIGN_CHANNELS = ["email", "social_media", "search", "display"]
CAMPAIGN_TYPES = ["brand_awareness", "product_launch", "seasonal", "retargeting"]
ADVERTISING_PLATFORMS = ["Google Ads", "Facebook Ads", "Instagram Ads", "Email Campaign"]
REVENUE_VALUES = [1700, 2200]
AGE_RANGE = (18, 65)
INCOME_RANGE = (1000, 10000)
DATE_START = datetime(2025, 8, 6)
DATE_END = datetime(2025, 9, 6)


def generate_raw_data(num_customers: int, output_dir: str) -> None:
    """Generate synthetic marketing data and save it to the bronze layer.

    Args:
        num_customers (int): Number of customer records to generate.
        output_dir (str): Directory to save the output files.

    Raises:
        OSError: If there is an issue creating the output directory or saving files.
    """
    logger.info(f"Generating {num_customers} customer records...")
    data = []

    for _ in range(num_customers):
        customer_id = fake.uuid4()
        age = random.randint(*AGE_RANGE)
        gender = random.choice(["M", "F"])
        income = round(random.uniform(*INCOME_RANGE), 2)
        campaign_id = random.choice(CAMPAIGN_IDS)
        campaign_channel = random.choice(CAMPAIGN_CHANNELS)
        campaign_type = random.choice(CAMPAIGN_TYPES)

        # Generate impressions based on campaign channel
        if campaign_channel == "display":
            impressions = random.randint(5, 35)
        elif campaign_channel == "social_media":
            impressions = random.randint(5, 25)
        elif campaign_channel == "email":
            impressions = random.randint(1, 15)
        else:  # search
            impressions = random.randint(1, 10)

        # Generate conversions (15% chance of conversion)
        conversions = random.randint(1, 2) if random.random() < 0.15 else 0

        # Calculate revenue based on conversions
        revenue = conversions * random.choice(REVENUE_VALUES)

        # Generate clicks, ensuring at least 10 if there are conversions
        clicks = (
            random.randint(max(10, conversions), 32)
            if conversions > 0
            else 0 if random.random() < 0.50 else random.randint(1, 32)
        )

        # Generate website visits (only if there are clicks)
        website_visits = 0 if clicks == 0 else random.randint(1, min(clicks, 3))

        # Generate time on site (only if there are website visits)
        time_on_site = 0 if website_visits == 0 else random.randint(60, 600)

        # Generate previous purchases (none for retargeting campaigns)
        previous_purchases = 0 if campaign_type == "retargeting" else random.randint(0, 2)

        # Calculate ad spend based on impressions
        ad_spend = round(impressions * random.uniform(0.05, 0.20), 2)

        # Generate additional fields
        date_received = fake.date_time_between(
            start_date=DATE_START, end_date=DATE_END
        )
        advertising_platform = random.choice(ADVERTISING_PLATFORMS)
        extraction_date = datetime.now()

        # Append record to data list
        data.append({
            "customer_id": customer_id,
            "age": age,
            "gender": gender,
            "income": income,
            "campaign_id": campaign_id,
            "campaign_channel": campaign_channel,
            "campaign_type": campaign_type,
            "ad_spend": ad_spend,
            "impressions": impressions,
            "clicks": clicks,
            "conversions": conversions,
            "revenue": revenue,
            "website_visits": website_visits,
            "time_on_site": time_on_site,
            "previous_purchases": previous_purchases,
            "date_received": date_received,
            "advertising_platform": advertising_platform,
            "extraction_date": extraction_date,
        })

    # Create DataFrame
    df = pd.DataFrame(data)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger.info(f"Saving data with timestamp {timestamp}...")

    try:
        # Save as Parquet
        parquet_path = os.path.join(output_dir, f"marketing_{timestamp}.parquet")
        df.to_parquet(parquet_path, index=False)
        logger.info(f"Data saved to {parquet_path}")

        # Save as CSV
        csv_path = os.path.join(output_dir, f"marketing_{timestamp}.csv")
        df.to_csv(csv_path, index=False)
        logger.info(f"Data saved to {csv_path}")
    except OSError as e:
        logger.error(f"Failed to save data: {e}")
        raise


if __name__ == "__main__":
    # Ensure output directory exists
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        logger.info(f"Output directory {OUTPUT_DIR} ensured.")
    except OSError as e:
        logger.error(f"Failed to create output directory {OUTPUT_DIR}: {e}")
        raise

    # Generate data for each customer count
    for num in NUM_CUSTOMERS:
        generate_raw_data(num, OUTPUT_DIR)