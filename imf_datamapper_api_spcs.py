#!/usr/bin/env python3

import os
import sys
from datetime import datetime, timezone
import requests
import pandas as pd
from snowflake.snowpark import Session
from snowpipe_streaming import SnowpipeStreamingClient

BASE_URL = "https://www.imf.org/external/datamapper/api/v1"
DATASET = "WEO"
TIMEOUT_SECONDS = 30

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_HOST = os.getenv("SNOWFLAKE_HOST")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

TABLE_NAME = "IMF_DATAMAPPER_INDICATORS"


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


def parse_indicator_data(indicator_code, data, ingestion_timestamp):
    values = data.get("values", {}).get(indicator_code, {})
    
    rows = []
    for country, yearly_data in values.items():
        for year, value in yearly_data.items():
            rows.append({
                "INDICATOR": indicator_code,
                "COUNTRY_CODE": country,
                "YEAR": int(year),
                "VALUE": float(value) if value is not None else None,
                "INGESTION_TIMESTAMP": ingestion_timestamp
            })
    
    return pd.DataFrame(rows)


def write_to_snowflake_pandas(session, df):
    session.write_pandas(
        df,
        table_name=TABLE_NAME,
        auto_create_table=True,
        overwrite=True
    )


def write_to_snowflake_streaming(df):
    config = {
        "account": SNOWFLAKE_ACCOUNT,
        "host": SNOWFLAKE_HOST,
        "token": get_login_token(),
        "token_type": "OAUTH",
        "database": SNOWFLAKE_DATABASE,
        "schema": SNOWFLAKE_SCHEMA
    }
    
    client = SnowpipeStreamingClient(config)
    channel = None
    
    try:
        channel = client.open_channel(f"{TABLE_NAME}_CHANNEL", TABLE_NAME)
        
        rows = df.to_dict("records")
        response = channel.append_rows(rows)
        
        if response.has_errors():
            print(f"Errors: {response.get_errors()}", file=sys.stderr)
        else:
            print(f"Streamed {len(rows)} rows")
    finally:
        if channel:
            channel.close()
        client.close()


def main():
    write_method = os.getenv("WRITE_METHOD", "print")
    ingestion_timestamp = datetime.now(timezone.utc)
    
    try:
        weo_indicators = fetch_weo_indicators()
                
        all_data = []
        for indicator_code in weo_indicators:
            data = fetch_indicator_data(indicator_code)
            df = parse_indicator_data(indicator_code, data, ingestion_timestamp)
            all_data.append(df)
        
        combined_df = pd.concat(all_data, ignore_index=True)
        
        if write_method == "print":
            print(combined_df.head())
        elif write_method == "streaming":
            write_to_snowflake_streaming(combined_df)
            print("Data written using streaming method")
        elif write_method == "pandas":
            session = get_snowpark_session()
            write_to_snowflake_pandas(session, combined_df)
            session.close()
            print("Data written using pandas method")
        
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
