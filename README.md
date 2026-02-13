# IMF DataMapper - Snowflake Ingestion Examples

Python examples for loading [IMF World Economic Outlook](https://www.imf.org/external/datamapper/api/v1/) (WEO) data into Snowflake using three different ingestion methods.

## Deployment Options

| Option | Script | Ingestion Method | Runs In |
|--------|--------|------------------|---------|
| [Stored Procedure](docs/stored-procedure.md) | `imf_datamapper_api_proc.py` | write_pandas (replace) | Snowflake |
| [SPCS Container](docs/spcs-container.md) | `imf_datamapper_api_spcs.py` | write_pandas (replace) | SPCS compute pool |
| [Snowpipe Streaming v2](docs/snowpipe-streaming-v2.md) | `imf_datamapper_api_ssv2.py` | Snowpipe Streaming SDK (staging + swap) | AWS Lambda / standalone |

**Stored Procedure** is the simplest option — a single Python file deployed as a Snowflake stored procedure. Best for scheduled batch refreshes.

**SPCS Container** runs the same logic inside Snowpark Container Services as a batch job or continuous service. Useful when you need more compute control or container-based workflows.

**Snowpipe Streaming v2** uses the high-performance Snowpipe Streaming SDK with key-pair auth. Designed for external runtimes (AWS Lambda, EC2, etc.) and uses a staging table + atomic swap pattern to replace data on each run.

## Files

| File | Description |
|------|-------------|
| `imf_datamapper_api_proc.py` | Stored procedure version (write_pandas) |
| `imf_datamapper_api_spcs.py` | SPCS container version (write_pandas) |
| `imf_datamapper_api_ssv2.py` | Snowpipe Streaming v2 version (staging + swap) |
| `Dockerfile` | Container image for SPCS |
| `spec.yaml` | SPCS service specification |
| `build.sh` | Script to build and tag Docker image |
| `snowflake_worksheet.sql` | Example queries for the loaded data |
| `profile.json.template` | Template for Snowpipe Streaming credentials |

## Data Source

- **API**: [IMF DataMapper API](https://www.imf.org/external/datamapper/api/v1/)
- **Dataset**: World Economic Outlook (WEO)
- **Indicators**: GDP, GDP per capita, inflation, unemployment, government debt, population, and more
- **Target table**: `API_DEMO.PUBLIC.IMF_DATAMAPPER_INDICATORS`

| Column | Type | Description |
|--------|------|-------------|
| `INDICATOR` | VARCHAR | WEO indicator code (e.g., NGDPDPC, PCPIPCH) |
| `COUNTRY_CODE` | VARCHAR | ISO country code |
| `YEAR` | INTEGER | Data year |
| `VALUE` | FLOAT | Indicator value |
| `INGESTION_TIMESTAMP` | TIMESTAMP_NTZ | When the data was loaded |

## Local Development

```bash
uv venv
uv pip install -r requirements.txt
uv run python imf_datamapper_api_ssv2.py
```
