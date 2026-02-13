#!/usr/bin/env python3
"""
IMF DataMapper API - Snowpipe Streaming v2 Version
Fetches World Economic Outlook data from IMF and loads into Snowflake
using Snowpipe Streaming high-performance SDK.

Uses a staging table + atomic swap pattern to replace data on each run.

Designed to run as an AWS Lambda function or standalone script.
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import requests
import pandas as pd
import snowflake.connector
from snowflake.ingest.streaming import StreamingIngestClient

load_dotenv()

# Configure logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
)
logger = logging.getLogger(__name__)

BASE_URL = "https://www.imf.org/external/datamapper/api/v1"
DATASET = "WEO"
TIMEOUT_SECONDS = 120

SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "API_DEMO")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
TABLE_NAME = os.getenv("TABLE_NAME", "IMF_DATAMAPPER_INDICATORS")
STAGING_TABLE_NAME = f"{TABLE_NAME}_STAGING"
PROFILE_JSON_PATH = os.getenv("PROFILE_JSON_PATH", "profile.json")

POLL_ATTEMPTS = 30
POLL_INTERVAL_SECONDS = 1


def load_config():
    """Load connection config from profile.json."""
    with open(PROFILE_JSON_PATH, 'r') as f:
        config = json.load(f)
    safe_config = {k: v for k, v in config.items() if 'key' not in k.lower()}
    logger.debug(f"Profile config: {safe_config}")
    return config


def get_snowflake_connection(config):
    """Create a Snowflake connector connection for DDL operations."""
    return snowflake.connector.connect(
        account=config["account"],
        user=config["user"],
        private_key_file=config["private_key_file"],
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=config.get("role"),
    )


def prepare_staging_table(conn):
    """Create or truncate the staging table (cloned from production schema)."""
    cur = conn.cursor()
    try:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {STAGING_TABLE_NAME}
            LIKE {TABLE_NAME}
        """)
        cur.execute(f"TRUNCATE TABLE {STAGING_TABLE_NAME}")
        logger.info(f"Staging table {STAGING_TABLE_NAME} ready")
    finally:
        cur.close()


def swap_tables(conn):
    """Atomically swap staging table into production."""
    cur = conn.cursor()
    try:
        cur.execute(f"""
            ALTER TABLE {TABLE_NAME}
            SWAP WITH {STAGING_TABLE_NAME}
        """)
        logger.info(f"Swapped {STAGING_TABLE_NAME} -> {TABLE_NAME}")
    finally:
        cur.close()


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


def write_streaming(df):
    """Write DataFrame to the staging table using Snowpipe Streaming v2."""
    config = load_config()
    pipe_name = f"{STAGING_TABLE_NAME}-STREAMING"

    logger.info(f"Creating client for {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{pipe_name}")

    client = StreamingIngestClient(
        client_name="imf_datamapper",
        db_name=SNOWFLAKE_DATABASE,
        schema_name=SNOWFLAKE_SCHEMA,
        pipe_name=pipe_name,
        properties=config
    )
    logger.info("Client created successfully")

    try:
        channel, _ = client.open_channel("imf_channel")
        logger.info("Channel opened successfully")

        rows = df.to_dict("records")
        row_count = len(rows)
        end_offset = str(row_count - 1)
        logger.info(f"Inserting {row_count} rows...")

        for i, row in enumerate(rows):
            channel.append_row(row, str(i))
            if (i + 1) % 10000 == 0:
                logger.debug(f"Appended {i + 1}/{row_count} rows")

        logger.info("All rows appended, waiting for commit...")
        for attempt in range(POLL_ATTEMPTS):
            if channel.get_latest_committed_offset_token() == end_offset:
                logger.info("All rows committed successfully")
                return True
            time.sleep(POLL_INTERVAL_SECONDS)
            logger.debug(f"Poll attempt {attempt + 1}/{POLL_ATTEMPTS}")

        logger.error("Timeout waiting for rows to commit")
        return False

    except Exception as e:
        logger.error(f"Streaming error: {e}", exc_info=True)
        return False

    finally:
        if not client.is_closed():
            client.close()


def main():
    """Fetch IMF WEO data and stream into Snowflake. Returns a summary dict."""
    ingestion_timestamp = datetime.now(timezone.utc)
    config = load_config()

    # Fetch data from IMF API
    weo_indicators = fetch_weo_indicators()

    all_data = []
    for indicator_code in weo_indicators:
        data = fetch_indicator_data(indicator_code)
        df = parse_indicator_data(indicator_code, data)
        all_data.append(df)

    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df["INGESTION_TIMESTAMP"] = ingestion_timestamp.isoformat()

    # Prepare staging table, stream data, then swap
    conn = get_snowflake_connection(config)
    try:
        prepare_staging_table(conn)

        if not write_streaming(combined_df):
            raise RuntimeError("Failed to stream rows to staging table")

        swap_tables(conn)
    finally:
        conn.close()

    msg = f"Streamed {len(combined_df)} rows into {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TABLE_NAME}"
    logger.info(msg)
    return {"statusCode": 200, "body": msg}


def lambda_handler(event, context):
    """AWS Lambda entry point."""
    return main()


if __name__ == "__main__":
    main()
