# Snowpipe Streaming v2 Deployment

Deploys `imf_datamapper_api_ssv2.py` as a standalone script or AWS Lambda function using the [Snowpipe Streaming high-performance SDK](https://docs.snowflake.com/en/user-guide/snowpipe-streaming-high-performance-getting-started) to load IMF WEO data.

Uses a **staging table + atomic swap** pattern to replace data on each run — the production table is never empty during a refresh.

> **Recommended: Use Cortex Code CLI**
> 
> The easiest way to set up the Snowflake objects is using [Cortex Code CLI](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-cli), Snowflake's AI-powered CLI assistant.
>
> **Example Prompt:**
> ```
> Read the README and docs/snowpipe-streaming-v2.md for deployment details,
> then create the target table and staging table for the SSv2 ingestion script.
> Set up key-pair authentication for my user and verify the configuration.
> ```

## Prerequisites

- Snowflake account with ACCOUNTADMIN or equivalent privileges
- Python 3.11+ with [uv](https://docs.astral.sh/uv/) (or pip)
- RSA key pair for Snowflake key-pair authentication

## How It Works

1. Fetches all WEO indicators from the IMF DataMapper API
2. Creates/truncates a staging table (`IMF_DATAMAPPER_INDICATORS_STAGING`)
3. Streams rows into the staging table via Snowpipe Streaming v2
4. Atomically swaps the staging table with production (`ALTER TABLE ... SWAP WITH`)

The swap is instant — readers always see complete data in the production table.

## Setup

### Step 1: Create Target Table

```sql
CREATE TABLE IF NOT EXISTS API_DEMO.PUBLIC.IMF_DATAMAPPER_INDICATORS (
    INDICATOR VARCHAR,
    COUNTRY_CODE VARCHAR,
    YEAR INTEGER,
    VALUE FLOAT,
    INGESTION_TIMESTAMP TIMESTAMP_NTZ
);
```

### Step 2: Generate RSA Key Pair

```bash
# Generate private key (unencrypted PKCS8)
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt

# Extract public key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

### Step 3: Assign Public Key to Snowflake User

```sql
ALTER USER <username> SET RSA_PUBLIC_KEY='<paste public key without headers/footers>';
```

### Step 4: Create profile.json

```json
{
  "url": "https://<account_identifier>.snowflakecomputing.com",
  "account": "<account_identifier>",
  "user": "<username>",
  "private_key_file": "rsa_key.p8",
  "role": "<role>"
}
```

### Step 5: Create .env

```
SNOWFLAKE_DATABASE=API_DEMO
SNOWFLAKE_SCHEMA=PUBLIC
TABLE_NAME=IMF_DATAMAPPER_INDICATORS
PROFILE_JSON_PATH=profile.json
LOG_LEVEL=DEBUG
```

### Step 6: Install Dependencies

```bash
uv venv
uv pip install -r requirements.txt
```

## Run

```bash
uv run python imf_datamapper_api_ssv2.py
```

Expected output:

```
INFO | Creating client for API_DEMO.PUBLIC.IMF_DATAMAPPER_INDICATORS_STAGING-STREAMING
INFO | Client created successfully
INFO | Channel opened successfully
INFO | Inserting 147392 rows...
INFO | All rows appended, waiting for commit...
INFO | All rows committed successfully
INFO | Swapped IMF_DATAMAPPER_INDICATORS_STAGING -> IMF_DATAMAPPER_INDICATORS
INFO | Streamed 147392 rows into API_DEMO.PUBLIC.IMF_DATAMAPPER_INDICATORS
```

## AWS Lambda Deployment

The script includes a `lambda_handler(event, context)` entry point. To deploy:

1. Package the script with its dependencies
2. Set the handler to `imf_datamapper_api_ssv2.lambda_handler`
3. Configure environment variables (`SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `TABLE_NAME`, `PROFILE_JSON_PATH`)
4. Bundle `profile.json` and `rsa_key.p8` with the deployment package (or use AWS Secrets Manager)

## Verify Data

```sql
SELECT COUNT(*) AS ROW_COUNT, 
       COUNT(DISTINCT INDICATOR) AS INDICATORS,
       COUNT(DISTINCT COUNTRY_CODE) AS COUNTRIES,
       MIN(YEAR) AS MIN_YEAR,
       MAX(YEAR) AS MAX_YEAR
FROM API_DEMO.PUBLIC.IMF_DATAMAPPER_INDICATORS;
```

## Cleanup

```sql
DROP TABLE IF EXISTS API_DEMO.PUBLIC.IMF_DATAMAPPER_INDICATORS;
DROP TABLE IF EXISTS API_DEMO.PUBLIC.IMF_DATAMAPPER_INDICATORS_STAGING;
```
