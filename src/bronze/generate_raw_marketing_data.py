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

# Initialize Faker
fake = Faker()

# Configuration constants
NUM_CUSTOMERS = [5000]
OUTPUT_DIR = "data/bronze"
CAMPAIGN_IDS = [
    "92c14ef8-a59a-4a78-9670-9e527d9947a1",
    "4c967788-de7b-4626-bbca-c7e7144da864",
]
CAMPAIGN_CHANNELS = ["email", "social_media", "search", "display"]
CAMPAIGN_TYPES = ["product_launch"]
ADVERTISING_PLATFORMS = ["Google Ads", "Facebook Ads", "Instagram Ads", "Email Campaign"]

# Mapping of campaign_channel to advertising_platform
CHANNEL_TO_PLATFORM = {
    "email": ["Email Campaign"],
    "social_media": ["Facebook Ads", "Instagram Ads"],
    "search": ["Google Ads"],
    "display": ["Google Ads", "Facebook Ads", "Instagram Ads"]
}

# Revenue values for conversions
REVENUE_VALUES = [300, 500, 800, 1200, 2000]

AGE_RANGE = (18, 65)
INCOME_RANGE = (1000, 10000)
DATE_START = datetime(2025, 8, 6)
DATE_END = datetime(2025, 9, 6)

# Costs per channel (in BRL)
COST_PER_EMAIL = 0.20  # Cost per email sent
CPM_SOCIAL_DISPLAY = 49.34  # Cost per thousand impressions (Meta/Google)
CPC_SEARCH = 22.23  # Cost per click (Google Ads)

def generate_raw_data(num_customers: int, output_dir: str) -> None:
    """Generate synthetic marketing data and save to bronze layer."""
    logger.info(f"Generating {num_customers} customer records...")
    data = []

    for i in range(num_customers):
        customer_id = fake.uuid4()
        age = random.randint(*AGE_RANGE)
        gender = random.choice(["M", "F"])
        income = round(random.uniform(*INCOME_RANGE), 2)
        campaign_id = random.choice(CAMPAIGN_IDS)
        campaign_channel = random.choice(CAMPAIGN_CHANNELS)
        campaign_type = random.choice(CAMPAIGN_TYPES)

        # Select advertising_platform based on campaign_channel
        advertising_platform = random.choice(CHANNEL_TO_PLATFORM[campaign_channel])

        # Impressions vary by channel
        if campaign_channel == "display":
            impressions = random.randint(5, 35)
        elif campaign_channel == "social_media":
            impressions = random.randint(5, 25)
        elif campaign_channel == "email":
            impressions = random.randint(1, 15)
        else:  # search
            impressions = random.randint(1, 10)

        # Clicks based on channel-specific CTR
        ctr_lookup = {
            "display": 0.01,
            "social_media": 0.05,
            "email": 0.12,
            "search": 0.08
        }
        ctr = ctr_lookup[campaign_channel]
        clicks = min(impressions, max(0, int(np.random.binomial(impressions, ctr))))

        # Website visits: 60–85% of clicks
        if clicks == 0:
            website_visits = 0
        else:
            website_visits = sum(1 for _ in range(clicks) if random.random() < random.uniform(0.6, 0.85))

        # Time on site (seconds) if there are visits
        time_on_site = 0 if website_visits == 0 else random.randint(60, 600)

        # Conversions: 3% chance if there are visits
        conversions = 0
        if website_visits > 0:
            if random.random() < 0.03:
                conversions = random.choices([1, 2], weights=[0.85, 0.15])[0]

        # Revenue based on conversions and ticket value
        revenue = conversions * random.choice(REVENUE_VALUES)

        # Previous purchases
        previous_purchases = random.randint(0, 2)

        # Ad spend based on channel
        if campaign_channel == "email":
            ad_spend = round(impressions * COST_PER_EMAIL, 2)
        elif campaign_channel == "search":
            ad_spend = round(clicks * CPC_SEARCH, 2)
        else:  # social_media and display
            ad_spend = round((impressions / 1000) * CPM_SOCIAL_DISPLAY, 2)

        # Dates
        date_received = fake.date_time_between(start_date=DATE_START, end_date=DATE_END)
        extraction_date = datetime.now()

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

    # Create DataFrame and save
    df = pd.DataFrame(data)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger.info(f"Saving data with timestamp {timestamp}...")

    try:
        os.makedirs(output_dir, exist_ok=True)
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
    for num in NUM_CUSTOMERS:
        generate_raw_data(num, OUTPUT_DIR)
