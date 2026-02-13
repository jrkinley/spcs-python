# Stored Procedure Deployment

Deploys `imf_datamapper_api_proc.py` as a Snowflake stored procedure using `write_pandas` to load IMF WEO data.

> **Recommended: Use Cortex Code CLI**
> 
> The easiest way to deploy is using [Cortex Code CLI](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-cli), Snowflake's AI-powered CLI assistant. Simply describe what you want and Cortex will handle creating all required objects automatically.
>
> **Example Prompt:**
> ```
> Read the README and docs/stored-procedure.md for deployment details, then
> deploy imf_datamapper_api_proc.py as a stored procedure. Create the necessary
> network rule and external access integration, upload the code to a stage, and
> create the procedure. Run it and verify the data loaded correctly.
> ```

## Prerequisites

- Snowflake account with ACCOUNTADMIN or equivalent privileges
- Access to create stages, network rules, external access integrations, and stored procedures
- Snowflake CLI (`snow`) installed for uploading files to stages

## Manual Deployment Steps

Since the stored procedure calls an external API (IMF DataMapper), you need to create the following objects in order:

### Step 1: Create Network Rule

Create a network rule to allow outbound connections to the IMF API:

```sql
CREATE OR REPLACE NETWORK RULE API_DEMO.PUBLIC.IMF_API_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('www.imf.org');
```

### Step 2: Create External Access Integration

Create an external access integration that uses the network rule:

```sql
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION IMF_API_ACCESS
  ALLOWED_NETWORK_RULES = (API_DEMO.PUBLIC.IMF_API_NETWORK_RULE)
  ENABLED = TRUE;
```

### Step 3: Create Stage for Python Code

```sql
CREATE OR REPLACE STAGE API_DEMO.PUBLIC.PYTHON_CODE
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Stage for Python stored procedure code';
```

### Step 4: Upload Python File to Stage

```bash
snow stage copy imf_datamapper_api_proc.py @API_DEMO.PUBLIC.PYTHON_CODE
```

Verify the upload:

```sql
LIST @API_DEMO.PUBLIC.PYTHON_CODE;
```

### Step 5: Create the Stored Procedure

```sql
CREATE OR REPLACE PROCEDURE API_DEMO.PUBLIC.IMF_DATAMAPPER_REFRESH()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests', 'pandas')
IMPORTS = ('@API_DEMO.PUBLIC.PYTHON_CODE/imf_datamapper_api_proc.py')
HANDLER = 'imf_datamapper_api_proc.main'
EXTERNAL_ACCESS_INTEGRATIONS = (IMF_API_ACCESS)
EXECUTE AS CALLER;
```

### Key Configuration

| Setting | Value | Purpose |
|---------|-------|---------|
| `RUNTIME_VERSION` | 3.11 | Python version |
| `PACKAGES` | snowflake-snowpark-python, requests, pandas | Required dependencies |
| `IMPORTS` | @API_DEMO.PUBLIC.PYTHON_CODE/imf_datamapper_api_proc.py | References Python file on stage |
| `HANDLER` | imf_datamapper_api_proc.main | Module.function format for imported files |
| `EXTERNAL_ACCESS_INTEGRATIONS` | IMF_API_ACCESS | Allows outbound API calls |
| `EXECUTE AS CALLER` | - | Uses caller's permissions for table access |

## Execute

```sql
CALL API_DEMO.PUBLIC.IMF_DATAMAPPER_REFRESH();
```

## Verify Data

```sql
SELECT COUNT(*) AS ROW_COUNT, 
       COUNT(DISTINCT INDICATOR) AS INDICATORS,
       COUNT(DISTINCT COUNTRY_CODE) AS COUNTRIES,
       MIN(YEAR) AS MIN_YEAR,
       MAX(YEAR) AS MAX_YEAR
FROM API_DEMO.PUBLIC.IMF_DATAMAPPER_INDICATORS;
```

## Updating

Re-upload the Python file and recreate the procedure:

```bash
snow stage copy imf_datamapper_api_proc.py @API_DEMO.PUBLIC.PYTHON_CODE --overwrite
```

Then re-run the `CREATE OR REPLACE PROCEDURE` statement above.

## Cleanup

```sql
DROP PROCEDURE IF EXISTS API_DEMO.PUBLIC.IMF_DATAMAPPER_REFRESH();
DROP STAGE IF EXISTS API_DEMO.PUBLIC.PYTHON_CODE;
DROP EXTERNAL ACCESS INTEGRATION IF EXISTS IMF_API_ACCESS;
DROP NETWORK RULE IF EXISTS API_DEMO.PUBLIC.IMF_API_NETWORK_RULE;
DROP TABLE IF EXISTS API_DEMO.PUBLIC.IMF_DATAMAPPER_INDICATORS;
```
