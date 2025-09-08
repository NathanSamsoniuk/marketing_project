## Script: `clean_transform_marketing.py`

### Purpose
This script processes raw marketing data from the `bronze` layer, applying the following transformations:
- Removes duplicates based on `customer_id`.
- Standardizes data types for all columns.
- Handles missing values in `income` and `ad_spend`.
- Validates that conversions do not exceed clicks.
- Adds a processing timestamp (`extraction_date`).

The transformed data is saved for further aggregation in the `gold` layer.

### Input
- The most recent Parquet file from `data/bronze` with prefix `marketing_` and extension `.parquet`.

### Output
- **Parquet file**: `data/silver/marketing_<timestamp>.parquet`
- **CSV file**: `data/silver/marketing_<timestamp>.csv`

The output retains the same columns as the input, with standardized types and an updated `extraction_date`:
- `customer_id`: String (UUID).
- `age`: Integer (18–65).
- `gender`: String ("M" or "F").
- `income`: Float (1000–10000, missing values filled with mean).
- `campaign_id`: String (UUID).
- `campaign_channel`: String ("email", "social_media", "search", "display").
- `campaign_type`: String ("brand_awareness", "product_launch", "seasonal", "retargeting").
- `ad_spend`: Float (missing values filled with 0).
- `impressions`: Integer.
- `clicks`: Integer.
- `conversions`: Integer (0–2).
- `revenue`: Integer.
- `website_visits`: Integer (0–3).
- `time_on_site`: Integer (0 or 60–600 seconds).
- `previous_purchases`: Integer (0–2).
- `date_received`: Datetime (between 2025-08-06 and 2025-09-06).
- `advertising_platform`: String ("Google Ads", "Facebook Ads", "Instagram Ads", "Email Campaign").
- `extraction_date`: Datetime (processing timestamp).