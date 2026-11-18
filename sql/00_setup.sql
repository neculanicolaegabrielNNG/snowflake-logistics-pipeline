/* ============================================================================
   00_setup.sql  -  Account-level objects
   Run once, as ACCOUNTADMIN (a trial account is fine).

   Creates: warehouse, resource monitor (cost guard-rail), a dedicated role,
            database and schemas.

   Layers:  RAW      -> data exactly as it arrived (all VARCHAR + audit columns)
            STAGING  -> cleaned, typed, de-duplicated
            MARTS    -> business-ready dimensions and facts
            ML       -> forecasting / anomaly detection objects
            OPS      -> procedures, tasks, run log, data quality checks
   ========================================================================== */

USE ROLE ACCOUNTADMIN;

-- 1. Compute: smallest warehouse, suspends after 60 s of inactivity ----------
CREATE WAREHOUSE IF NOT EXISTS LOGISTICS_WH
  WAREHOUSE_SIZE      = 'XSMALL'
  AUTO_SUSPEND        = 60
  AUTO_RESUME         = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT             = 'Compute for the logistics pipeline portfolio project';

-- 2. Cost guard-rail: notify at 80 %, suspend at 100 % of 10 credits/month ----
CREATE RESOURCE MONITOR IF NOT EXISTS LOGISTICS_RM
  WITH CREDIT_QUOTA = 10
  FREQUENCY = MONTHLY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 80  PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE LOGISTICS_WH SET RESOURCE_MONITOR = LOGISTICS_RM;

-- 3. Role: least privilege, everything in this project is owned by one role ----
CREATE ROLE IF NOT EXISTS LOGISTICS_ENGINEER
  COMMENT = 'Owns and operates the logistics pipeline';

GRANT ROLE LOGISTICS_ENGINEER TO ROLE SYSADMIN;          -- keep the role hierarchy clean
GRANT USAGE, OPERATE ON WAREHOUSE LOGISTICS_WH TO ROLE LOGISTICS_ENGINEER;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE LOGISTICS_ENGINEER;
GRANT EXECUTE TASK    ON ACCOUNT TO ROLE LOGISTICS_ENGINEER;  -- needed to run tasks

-- Also grant the role to the user that will run the rest of the scripts:
-- GRANT ROLE LOGISTICS_ENGINEER TO USER <your_user>;

-- 4. Database and schemas ------------------------------------------------------
USE ROLE LOGISTICS_ENGINEER;

CREATE DATABASE IF NOT EXISTS LOGISTICS_DB
  COMMENT = 'Synthetic parcel-delivery data for a Snowflake portfolio project';

CREATE SCHEMA IF NOT EXISTS LOGISTICS_DB.RAW     COMMENT = 'Landing zone: data as received';
CREATE SCHEMA IF NOT EXISTS LOGISTICS_DB.STAGING COMMENT = 'Cleaned and typed data';
CREATE SCHEMA IF NOT EXISTS LOGISTICS_DB.MARTS   COMMENT = 'Dimensions and facts for analytics';
CREATE SCHEMA IF NOT EXISTS LOGISTICS_DB.ML      COMMENT = 'ML functions: forecast, anomaly detection';
CREATE SCHEMA IF NOT EXISTS LOGISTICS_DB.OPS     COMMENT = 'Procedures, tasks, run log, DQ checks';

-- Sanity check
SHOW SCHEMAS IN DATABASE LOGISTICS_DB;
