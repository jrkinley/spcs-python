# SPCS Python

Snowpark Container Services and Stored Procedure examples for consuming IMF DataMapper API.

## Overview

This project demonstrates two deployment options for loading IMF World Economic Outlook (WEO) data into Snowflake:

1. **Stored Procedure** (`imf_datamapper_api_sp.py`) - Simple deployment using Cortex Code CLI
2. **Container Service** (`imf_datamapper_api.py`) - Full SPCS deployment with multiple output options

## Files

| File | Description |
|------|-------------|
| `imf_datamapper_api_sp.py` | Stored procedure version (uses write_pandas) |
| `imf_datamapper_api.py` | Full SPCS version with streaming support |
| `Dockerfile` | Container image for SPCS |
| `spec.yaml` | SPCS service specification |
| `build.sh` | Script to build and tag Docker image |

## Option 1: Stored Procedure Deployment

> **Recommended: Use Cortex Code CLI**
> 
> The easiest way to deploy this stored procedure is using [Cortex Code CLI](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-cli), Snowflake's AI-powered CLI assistant. Simply tell Cortex what you want to deploy and it will handle creating the network rules, external access integrations, stages, and stored procedures for you automatically.

### Prerequisites

- Snowflake account with ACCOUNTADMIN or equivalent privileges
- Access to create stages, network rules, external access integrations, and stored procedures
- Snowflake CLI (`snow`) installed for uploading files to stages

### Manual Deployment Steps

Since the stored procedure calls an external API (IMF DataMapper), you need to create the following objects in order:

#### Step 1: Create Network Rule

Create a network rule to allow outbound connections to the IMF API:

```sql
CREATE OR REPLACE NETWORK RULE API_DEMO.PUBLIC.IMF_API_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('www.imf.org');
```

#### Step 2: Create External Access Integration

Create an external access integration that uses the network rule:

```sql
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION IMF_API_ACCESS
  ALLOWED_NETWORK_RULES = (API_DEMO.PUBLIC.IMF_API_NETWORK_RULE)
  ENABLED = TRUE;
```

#### Step 3: Create Stage for Python Code

Create a stage to store the Python source file:

```sql
CREATE OR REPLACE STAGE API_DEMO.PUBLIC.PYTHON_CODE
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Stage for Python stored procedure code';
```

#### Step 4: Upload Python File to Stage

Upload the Python file using Snowflake CLI:

```bash
snow stage copy imf_datamapper_api_sp.py @API_DEMO.PUBLIC.PYTHON_CODE
```

Verify the upload:

```sql
LIST @API_DEMO.PUBLIC.PYTHON_CODE;
```

#### Step 5: Create the Stored Procedure

Create the stored procedure that imports the Python file from the stage:

```sql
CREATE OR REPLACE PROCEDURE API_DEMO.PUBLIC.IMF_DATAMAPPER_REFRESH()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests', 'pandas')
IMPORTS = ('@API_DEMO.PUBLIC.PYTHON_CODE/imf_datamapper_api_sp.py')
HANDLER = 'imf_datamapper_api_sp.main'
EXTERNAL_ACCESS_INTEGRATIONS = (IMF_API_ACCESS)
EXECUTE AS CALLER;
```

### Key Configuration Notes

| Setting | Value | Purpose |
|---------|-------|---------|
| `RUNTIME_VERSION` | 3.11 | Python version |
| `PACKAGES` | snowflake-snowpark-python, requests, pandas | Required dependencies |
| `IMPORTS` | @API_DEMO.PUBLIC.PYTHON_CODE/imf_datamapper_api_sp.py | References Python file on stage |
| `HANDLER` | imf_datamapper_api_sp.main | Module.function format for imported files |
| `EXTERNAL_ACCESS_INTEGRATIONS` | IMF_API_ACCESS | Allows outbound API calls |
| `EXECUTE AS CALLER` | - | Uses caller's permissions for table access |

### Updating the Stored Procedure

To update the stored procedure code, simply re-upload the Python file to the stage:

```bash
snow stage copy imf_datamapper_api_sp.py @API_DEMO.PUBLIC.PYTHON_CODE --overwrite
```

Then recreate the procedure (the CREATE OR REPLACE statement remains the same):

```sql
CREATE OR REPLACE PROCEDURE API_DEMO.PUBLIC.IMF_DATAMAPPER_REFRESH()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests', 'pandas')
IMPORTS = ('@API_DEMO.PUBLIC.PYTHON_CODE/imf_datamapper_api_sp.py')
HANDLER = 'imf_datamapper_api_sp.main'
EXTERNAL_ACCESS_INTEGRATIONS = (IMF_API_ACCESS)
EXECUTE AS CALLER;
```

### Execute the Procedure

```sql
CALL API_DEMO.PUBLIC.IMF_DATAMAPPER_REFRESH();
```

### Verify Data

```sql
-- Check row count and data summary
SELECT COUNT(*) AS ROW_COUNT, 
       COUNT(DISTINCT INDICATOR) AS INDICATORS,
       COUNT(DISTINCT COUNTRY_CODE) AS COUNTRIES,
       MIN(YEAR) AS MIN_YEAR,
       MAX(YEAR) AS MAX_YEAR
FROM API_DEMO.PUBLIC.IMF_DATAMAPPER_INDICATORS;

-- List all indicators
SELECT DISTINCT INDICATOR 
FROM API_DEMO.PUBLIC.IMF_DATAMAPPER_INDICATORS 
ORDER BY INDICATOR;

-- Sample data
SELECT * FROM API_DEMO.PUBLIC.IMF_DATAMAPPER_INDICATORS LIMIT 10;
```

### Cleanup (if needed)

To remove all created objects:

```sql
DROP PROCEDURE IF EXISTS API_DEMO.PUBLIC.IMF_DATAMAPPER_REFRESH();
DROP STAGE IF EXISTS API_DEMO.PUBLIC.PYTHON_CODE;
DROP EXTERNAL ACCESS INTEGRATION IF EXISTS IMF_API_ACCESS;
DROP NETWORK RULE IF EXISTS API_DEMO.PUBLIC.IMF_API_NETWORK_RULE;
DROP TABLE IF EXISTS API_DEMO.PUBLIC.IMF_DATAMAPPER_INDICATORS;
```

## Option 2: SPCS Container Deployment

### Build and Push Container

```bash
# Update build.sh with your account details
./build.sh

# Login to Snowflake registry
docker login <org>-<account>.registry.snowflakecomputing.com

# Push the image
docker push <repo_url>
```

## Data Source

- **API**: [IMF DataMapper API](https://www.imf.org/external/datamapper/api/v1/)
- **Dataset**: World Economic Outlook (WEO)
- **Indicators**: GDP, Inflation, Unemployment, etc.

## Local Development

```bash
# Create UV environment
uv venv
uv pip install -r requirements.txt

# Run locally (print mode)
uv run python imf_datamapper_api.py
```
