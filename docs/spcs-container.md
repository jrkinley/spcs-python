# SPCS Container Deployment

Deploys `imf_datamapper_api_spcs.py` as a Snowpark Container Services (SPCS) batch job using `write_pandas` to load IMF WEO data.

> **Recommended: Use Cortex Code CLI**
> 
> The easiest way to deploy is using [Cortex Code CLI](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-cli), Snowflake's AI-powered CLI assistant.
>
> **Example Prompt:**
> ```
> Read the README and docs/spcs-container.md for deployment details, then deploy
> imf_datamapper_api_spcs.py as an SPCS container job. Create the image
> repository, compute pool, network rule and external access integration. Build
> and push the Docker image, execute the job, and verify the results.
> ```

## Prerequisites

- Snowflake account with ACCOUNTADMIN or equivalent privileges
- Docker installed locally
- Snowflake CLI (`snow`) installed

## Service Types

| Type | Lifespan | Restart | Use Case | Billing |
|------|----------|---------|----------|---------|
| `EXECUTE JOB SERVICE` | Runs until containers exit | No restart | Batch jobs, ETL | Only while running |
| `CREATE SERVICE` | Runs continuously until stopped | Auto-restarts | Web apps, APIs | Continuous |

This example uses `EXECUTE JOB SERVICE` for a batch data refresh job.

## Manual Deployment Steps

### Step 1: Create Network Rule and External Access Integration

If you already completed the [Stored Procedure](stored-procedure.md) setup, these objects already exist. Verify:

```sql
SHOW EXTERNAL ACCESS INTEGRATIONS LIKE 'IMF_API_ACCESS';
```

If they don't exist, create them:

```sql
CREATE OR REPLACE NETWORK RULE API_DEMO.PUBLIC.IMF_API_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('www.imf.org:443');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION IMF_API_ACCESS
  ALLOWED_NETWORK_RULES = (API_DEMO.PUBLIC.IMF_API_NETWORK_RULE)
  ENABLED = TRUE;
```

### Step 2: Create Image Repository

```sql
USE DATABASE API_DEMO;
USE SCHEMA PUBLIC;

CREATE IMAGE REPOSITORY IF NOT EXISTS imf_images;

-- Get the repository URL
SHOW IMAGE REPOSITORIES LIKE 'imf_images';
```

Note the `repository_url` from the output (e.g., `<org>-<account>.registry.snowflakecomputing.com/api_demo/public/imf_images`).

### Step 3: Create Compute Pool

```sql
CREATE COMPUTE POOL IF NOT EXISTS imf_compute_pool
  MIN_NODES = 1
  MAX_NODES = 1
  INSTANCE_FAMILY = CPU_X64_XS;

-- Wait for ACTIVE status
DESCRIBE COMPUTE POOL imf_compute_pool;
```

### Step 4: Build and Push Container

```bash
# Build for linux/amd64 (required for SPCS)
docker build --platform linux/amd64 -t imf_datamapper_api_spcs:latest .

# Tag for Snowflake registry (replace with your repository_url)
docker tag imf_datamapper_api_spcs:latest <repository_url>/imf_datamapper_api_spcs:latest

# Login to Snowflake registry
docker login <org>-<account>.registry.snowflakecomputing.com -u <username>

# Push image
docker push <repository_url>/imf_datamapper_api_spcs:latest
```

### Step 5: Execute Job Service

Run as a one-time batch job:

```sql
EXECUTE JOB SERVICE
  IN COMPUTE POOL imf_compute_pool
  NAME = imf_datamapper_job
  EXTERNAL_ACCESS_INTEGRATIONS = (IMF_API_ACCESS)
  FROM SPECIFICATION $$
  spec:
    containers:
    - name: imf-datamapper
      image: /api_demo/public/imf_images/imf_datamapper_api_spcs:latest
      env:
        SNOWFLAKE_DATABASE: API_DEMO
        SNOWFLAKE_SCHEMA: PUBLIC
        TABLE_NAME: IMF_DATAMAPPER_INDICATORS
  $$;
```

Check job status:

```sql
CALL SYSTEM$GET_JOB_STATUS('imf_datamapper_job');
CALL SYSTEM$GET_JOB_LOGS('imf_datamapper_job', 'imf-datamapper');
```

## Alternative: Continuous Service

For services that need to run continuously (e.g., APIs, web apps):

```sql
CREATE SERVICE imf_datamapper_service
  IN COMPUTE POOL imf_compute_pool
  EXTERNAL_ACCESS_INTEGRATIONS = (IMF_API_ACCESS)
  FROM SPECIFICATION $$
  spec:
    containers:
    - name: imf-datamapper
      image: /api_demo/public/imf_images/imf_datamapper_api_spcs:latest
      env:
        SNOWFLAKE_DATABASE: API_DEMO
        SNOWFLAKE_SCHEMA: PUBLIC
        TABLE_NAME: IMF_DATAMAPPER_INDICATORS
  $$;

-- Manage the service
SHOW SERVICES;
ALTER SERVICE imf_datamapper_service SUSPEND;
DROP SERVICE imf_datamapper_service;
```

## Cleanup

```sql
DROP COMPUTE POOL IF EXISTS imf_compute_pool;
DROP IMAGE REPOSITORY IF EXISTS imf_images;
```
