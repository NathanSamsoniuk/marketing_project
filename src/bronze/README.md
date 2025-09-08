## Script: `generate_raw_marketing_data.py`

### Purpose
This script generates synthetic raw marketing data, including customer demographics, campaign metrics (impressions, clicks, conversions, etc.), and timestamps. The data is stored in the `data/bronze` directory for further processing in the `silver` and `gold` layers.

### Input
- No external input is required; the script uses predefined constants and the `Faker` library to generate synthetic data.

### Output
- **Parquet file**: `data/bronze/marketing_<timestamp>.parquet`
- **CSV file**: `data/bronze/marketing_<timestamp>.csv`

The data includes the following columns:
- `customer_id`: Unique identifier for the customer (UUID).
- `age`: Customer age (18–65).
- `gender`: Customer gender ("M" or "F").
- `income`: Customer income (1000–10000).
- `campaign_id`: Unique campaign identifier (UUID).
- `campaign_channel`: Channel used ("email", "social_media", "search", "display").
- `campaign_type`: Type of campaign ("brand_awareness", "product_launch", "seasonal", "retargeting").
- `ad_spend`: Cost of the campaign based on impressions.
- `impressions`: Number of times the ad was shown.
- `clicks`: Number of clicks on the ad.
- `conversions`: Number of conversions (0–2).
- `revenue`: Revenue generated from conversions.
- `website_visits`: Number of website visits (0–3).
- `time_on_site`: Time spent on the website in seconds (0 or 60–600).
- `previous_purchases`: Number of previous purchases (0–2, 0 for retargeting campaigns).
- `date_received`: Date the data was received (between 2025-08-06 and 2025-09-06).
- `advertising_platform`: Platform used ("Google Ads", "Facebook Ads", "Instagram Ads", "Email Campaign").
- `extraction_date`: Timestamp of data generation.