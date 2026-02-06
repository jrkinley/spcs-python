#!/usr/bin/env python3
"""
IMF DataMapper API - Snowpark Container Services Version
Fetches World Economic Outlook data from IMF and loads into Snowflake.
"""

import os
import sys
from datetime import datetime, timezone
import requests
import pandas as pd
from snowflake.snowpark import Session

BASE_URL = "https://www.imf.org/external/datamapper/api/v1"
DATASET = "WEO"
TIMEOUT_SECONDS = 30

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_HOST = os.getenv("SNOWFLAKE_HOST")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "API_DEMO")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
TABLE_NAME = os.getenv("TABLE_NAME", "IMF_DATAMAPPER_INDICATORS")


def get_login_token():
    with open("/snowflake/session/token", "r") as f:
        return f.read()


def get_snowpark_session():
    return Session.builder.configs({
        "account": SNOWFLAKE_ACCOUNT,
        "host": SNOWFLAKE_HOST,
        "authenticator": "oauth",
        "token": get_login_token(),
        "database": SNOWFLAKE_DATABASE,
        "schema": SNOWFLAKE_SCHEMA
    }).create()


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


def main():
    ingestion_timestamp = datetime.now(timezone.utc)
    
    try:
        session = get_snowpark_session()
        
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
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            auto_create_table=True,
            overwrite=True
        )
        
        print(f"Loaded {len(combined_df)} rows into {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TABLE_NAME}")
        session.close()
        
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
