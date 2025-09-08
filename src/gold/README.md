## Script: `calculate_kpi_marketing.py`

### Purpose
This script processes data from the `silver` layer, calculates marketing KPIs for each record, handles infinite/null values, and adds a processing timestamp. The resulting data is enriched with KPIs, maintaining the same granularity as the input, and is optimized for analysis and reporting.

### Input
- The most recent Parquet file from `data/silver` with prefix `marketing_` and extension `.parquet`.

### Output
- **Parquet file**: `data/gold/marketing_metrics_<timestamp>.parquet`
- **CSV file**: `data/gold/marketing_metrics_<timestamp>.csv`

The output includes all columns from the `silver` layer, plus the following calculated KPIs:
- `ctr`: Click-through rate (%): `(clicks / impressions) * 100`, rounded to 2 decimals.
- `cvr`: Conversion rate (%): `(conversions / clicks) * 100`, rounded to 2 decimals.
- `cpc`: Cost per click: `ad_spend / clicks`, rounded to 2 decimals.
- `cpa`: Cost per acquisition: `ad_spend / conversions`, rounded to 2 decimals.
- `roas`: Return on ad spend: `revenue / ad_spend`, rounded to 2 decimals.
- `margin`: Profit margin (%): `((revenue - ad_spend) / revenue) * 100`, rounded to 2 decimals.
- `extraction_date`: Datetime (processing timestamp).