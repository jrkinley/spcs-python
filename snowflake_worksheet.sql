-- =============================================================================
-- IMF DataMapper - Snowflake Worksheet
-- =============================================================================

USE DATABASE API_DEMO;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;

-- -----------------------------------------------------------------------------
-- 1. DEPLOY & RUN STORED PROCEDURE
-- -----------------------------------------------------------------------------
-- Prerequisites: network rule + EAI already created (see README)
-- Upload: snow stage copy imf_datamapper_api_proc.py @API_DEMO.PUBLIC.PYTHON_CODE

CALL API_DEMO.PUBLIC.IMF_DATAMAPPER_REFRESH();

-- -----------------------------------------------------------------------------
-- 2. VERIFY DATA
-- -----------------------------------------------------------------------------

SELECT COUNT(*)                     AS ROW_COUNT,
       COUNT(DISTINCT INDICATOR)    AS INDICATORS,
       COUNT(DISTINCT COUNTRY_CODE) AS COUNTRIES
  FROM IMF_DATAMAPPER_INDICATORS;

-- -----------------------------------------------------------------------------
-- 3. EXAMPLE QUERIES
-- -----------------------------------------------------------------------------

-- Top 10 countries by GDP per capita (latest year)
SELECT COUNTRY_CODE,
       YEAR,
       ROUND(VALUE, 2) AS GDP_PER_CAPITA_USD
  FROM IMF_DATAMAPPER_INDICATORS
 WHERE INDICATOR = 'NGDPDPC'
   AND YEAR = (SELECT MAX(YEAR)
                 FROM IMF_DATAMAPPER_INDICATORS
                WHERE INDICATOR = 'NGDPDPC'
                  AND VALUE IS NOT NULL)
   AND VALUE IS NOT NULL
 ORDER BY VALUE DESC
 LIMIT 10;

-- Top 10 countries by highest inflation (latest year)
SELECT COUNTRY_CODE,
       YEAR,
       ROUND(VALUE, 2) AS INFLATION_PCT
  FROM IMF_DATAMAPPER_INDICATORS
 WHERE INDICATOR = 'PCPIPCH'
   AND YEAR = (SELECT MAX(YEAR)
                 FROM IMF_DATAMAPPER_INDICATORS
                WHERE INDICATOR = 'PCPIPCH'
                  AND VALUE IS NOT NULL)
   AND VALUE IS NOT NULL
 ORDER BY VALUE DESC
 LIMIT 10;

-- Side-by-side comparison: USA vs China vs India (latest year, all indicators)
SELECT INDICATOR,
       MAX(CASE WHEN COUNTRY_CODE = 'USA' THEN ROUND(VALUE, 2) END) AS USA,
       MAX(CASE WHEN COUNTRY_CODE = 'CHN' THEN ROUND(VALUE, 2) END) AS CHINA,
       MAX(CASE WHEN COUNTRY_CODE = 'IND' THEN ROUND(VALUE, 2) END) AS INDIA
  FROM IMF_DATAMAPPER_INDICATORS
 WHERE YEAR = (SELECT MAX(YEAR)
                 FROM IMF_DATAMAPPER_INDICATORS
                WHERE INDICATOR = 'NGDPD'
                  AND VALUE IS NOT NULL)
   AND COUNTRY_CODE IN ('USA', 'CHN', 'IND')
 GROUP BY INDICATOR
 ORDER BY INDICATOR;
