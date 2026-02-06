#!/usr/bin/env python3
"""
IMF DataMapper API - Snowflake Stored Procedure Version
Fetches World Economic Outlook data from IMF and loads into Snowflake.
"""

from datetime import datetime, timezone
import requests
import pandas as pd
from snowflake.snowpark import Session

BASE_URL = "https://www.imf.org/external/datamapper/api/v1"
DATASET = "WEO"
DATABASE = "API_DEMO"
SCHEMA = "PUBLIC"
TABLE_NAME = "IMF_DATAMAPPER_INDICATORS"
TIMEOUT_SECONDS = 30


def fetch_weo_indicators():
    response = requests.get(f"{BASE_URL}/indicators", timeout=TIMEOUT_SECONDS)
    response.raise_for_status()
    indicators = response.json().get("indicators", {})
    return {
        code: info for code, info in indicators.items()
        if info.get("dataset") == DATASET and code
    }


def fetch_indicator_data(indicator_code):
    response = requests.get(f"{BASE_URL}/{indicator_code}", timeout=TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def parse_indicator_data(indicator_code, data):
    values = data.get("values", {}).get(indicator_code, {})
    rows = []
    for country, yearly_data in values.items():
        for year, value in yearly_data.items():
            rows.append({
                "INDICATOR": indicator_code,
                "COUNTRY_CODE": country,
                "YEAR": int(year),
                "VALUE": float(value) if value is not None else None
            })
    return pd.DataFrame(rows)


def main(session: Session) -> str:
    """
    Main entry point for the stored procedure.
    Fetches IMF WEO data and writes to Snowflake table.
    
    Args:
        session: Snowpark session provided by Snowflake
    
    Returns:
        Status message with row count
    """
    ingestion_timestamp = datetime.now(timezone.utc)
    
    # Set database and schema context
    session.sql(f"USE DATABASE {DATABASE}").collect()
    session.sql(f"USE SCHEMA {SCHEMA}").collect()
    
    weo_indicators = fetch_weo_indicators()
    
    all_data = []
    for indicator_code in weo_indicators:
        data = fetch_indicator_data(indicator_code)
        df = parse_indicator_data(indicator_code, data)
        all_data.append(df)
    
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df["INGESTION_TIMESTAMP"] = ingestion_timestamp
    
    session.write_pandas(
        combined_df,
        table_name=TABLE_NAME,
        database=DATABASE,
        schema=SCHEMA,
        auto_create_table=True,
        overwrite=True
    )
    
    return f"Loaded {len(combined_df)} rows into {DATABASE}.{SCHEMA}.{TABLE_NAME}"
