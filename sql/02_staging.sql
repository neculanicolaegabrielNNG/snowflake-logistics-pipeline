/* ============================================================================
   02_staging.sql  -  STAGING layer
   Run as LOGISTICS_ENGINEER, after 01_raw_tables_and_load.sql.

   * Small dimensions and shipments  -> VIEWS (always fresh, no maintenance)
   * The big event table             -> a real TABLE, filled incrementally from
                                         the stream by OPS.SP_STAGE_EVENTS
   * Cleaning rules applied here:
       - trim whitespace, upper-case codes
       - cast with TRY_ functions (a bad value becomes NULL instead of an error)
       - de-duplicate on the business key, keeping the most recently loaded row
   ========================================================================== */

USE ROLE LOGISTICS_ENGINEER;
USE WAREHOUSE LOGISTICS_WH;
USE SCHEMA LOGISTICS_DB.STAGING;

-- 1. Dimension views ---------------------------------------------------------------
CREATE OR REPLACE VIEW STG_HUBS AS
SELECT
  TRIM(HUB_ID)                AS HUB_ID,
  UPPER(TRIM(HUB_CODE))       AS HUB_CODE,
  TRIM(CITY)                  AS CITY,
  TRIM(COUNTY)                AS COUNTY,
  TRIM(REGION)                AS REGION,
  TRY_TO_DOUBLE(LATITUDE)     AS LATITUDE,
  TRY_TO_DOUBLE(LONGITUDE)    AS LONGITUDE,
  TRY_TO_NUMBER(CAPACITY_PER_DAY) AS CAPACITY_PER_DAY
FROM LOGISTICS_DB.RAW.RAW_HUBS
QUALIFY ROW_NUMBER() OVER (PARTITION BY HUB_ID ORDER BY _LOADED_AT DESC) = 1;

CREATE OR REPLACE VIEW STG_VEHICLES AS
SELECT
  TRIM(VEHICLE_ID)            AS VEHICLE_ID,
  UPPER(TRIM(PLATE_NUMBER))   AS PLATE_NUMBER,
  UPPER(TRIM(VEHICLE_TYPE))   AS VEHICLE_TYPE,
  TRY_TO_NUMBER(CAPACITY_KG)  AS CAPACITY_KG,
  TRIM(HUB_ID)                AS HUB_ID,
  UPPER(TRIM(FUEL_TYPE))      AS FUEL_TYPE,
  UPPER(TRIM(STATUS))         AS STATUS
FROM LOGISTICS_DB.RAW.RAW_VEHICLES
QUALIFY ROW_NUMBER() OVER (PARTITION BY VEHICLE_ID ORDER BY _LOADED_AT DESC) = 1;

CREATE OR REPLACE VIEW STG_DRIVERS AS
SELECT
  TRIM(DRIVER_ID)             AS DRIVER_ID,
  TRIM(FULL_NAME)             AS FULL_NAME,
  TRIM(HUB_ID)                AS HUB_ID,
  TRY_TO_DATE(HIRE_DATE, 'YYYY-MM-DD') AS HIRE_DATE,
  UPPER(TRIM(LICENSE_CATEGORY)) AS LICENSE_CATEGORY,
  TRY_TO_BOOLEAN(IS_ACTIVE)   AS IS_ACTIVE
FROM LOGISTICS_DB.RAW.RAW_DRIVERS
QUALIFY ROW_NUMBER() OVER (PARTITION BY DRIVER_ID ORDER BY _LOADED_AT DESC) = 1;

CREATE OR REPLACE VIEW STG_CUSTOMERS AS
SELECT
  TRIM(CUSTOMER_ID)           AS CUSTOMER_ID,
  TRIM(CUSTOMER_NAME)         AS CUSTOMER_NAME,
  UPPER(TRIM(SEGMENT))        AS SEGMENT,
  TRIM(HOME_HUB_ID)           AS HOME_HUB_ID,
  TRY_TO_NUMBER(CONTRACT_SLA_HOURS) AS CONTRACT_SLA_HOURS,
  TRY_TO_DATE(ONBOARDED_DATE, 'YYYY-MM-DD') AS ONBOARDED_DATE
FROM LOGISTICS_DB.RAW.RAW_CUSTOMERS
QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY _LOADED_AT DESC) = 1;

-- 2. Shipments view ----------------------------------------------------------------------
-- All timestamps are Europe/Bucharest local time, stored as TIMESTAMP_NTZ.
CREATE OR REPLACE VIEW STG_SHIPMENTS AS
SELECT
  TRIM(SHIPMENT_ID)           AS SHIPMENT_ID,
  TRIM(CUSTOMER_ID)           AS CUSTOMER_ID,
  TRIM(ORIGIN_HUB_ID)         AS ORIGIN_HUB_ID,
  TRIM(DESTINATION_HUB_ID)    AS DESTINATION_HUB_ID,
  TRIM(DESTINATION_CITY)      AS DESTINATION_CITY,
  UPPER(TRIM(DESTINATION_ZONE)) AS DESTINATION_ZONE,
  UPPER(TRIM(SERVICE_LEVEL))  AS SERVICE_LEVEL,
  TRY_TO_DECIMAL(WEIGHT_KG, 10, 2)          AS WEIGHT_KG,
  TRY_TO_DECIMAL(DECLARED_VALUE_RON, 12, 2) AS DECLARED_VALUE_RON,
  TRY_TO_DECIMAL(COD_AMOUNT_RON, 12, 2)     AS COD_AMOUNT_RON,
  (TRY_TO_DECIMAL(COD_AMOUNT_RON, 12, 2) IS NOT NULL) AS IS_COD,
  TRY_TO_TIMESTAMP_NTZ(CREATED_AT, 'YYYY-MM-DD HH24:MI:SS') AS CREATED_AT,
  TRY_TO_DATE(PROMISED_DELIVERY_DATE, 'YYYY-MM-DD')         AS PROMISED_DELIVERY_DATE
FROM LOGISTICS_DB.RAW.RAW_SHIPMENTS
QUALIFY ROW_NUMBER() OVER (PARTITION BY SHIPMENT_ID ORDER BY _LOADED_AT DESC) = 1;

-- 3. Events table (incremental) -------------------------------------------------------------
-- Primary key is informational only: Snowflake does not enforce it on standard
-- tables, which is why uniqueness is guaranteed by the MERGE and checked in OPS.V_DQ_CHECKS.
CREATE TABLE IF NOT EXISTS STG_SHIPMENT_EVENTS (
  EVENT_ID        VARCHAR       NOT NULL,
  SHIPMENT_ID     VARCHAR,
  EVENT_TYPE      VARCHAR,
  EVENT_TS        TIMESTAMP_NTZ,
  HUB_ID          VARCHAR,
  DRIVER_ID       VARCHAR,
  VEHICLE_ID      VARCHAR,
  FAILURE_REASON  VARCHAR,
  SOURCE_FILE     VARCHAR,
  LOADED_AT       TIMESTAMP_NTZ,
  CONSTRAINT PK_STG_SHIPMENT_EVENTS PRIMARY KEY (EVENT_ID)
)
COMMENT ='Cleaned, de-duplicated scan events. Filled by OPS.SP_STAGE_EVENTS from the RAW stream.';

-- 4. Ops: run log + the incremental merge procedure ------------------------------------------
USE SCHEMA LOGISTICS_DB.OPS;

CREATE TABLE IF NOT EXISTS PIPELINE_RUN_LOG (
  RUN_ID         NUMBER AUTOINCREMENT,
  PIPELINE_STEP  VARCHAR,
  ROWS_AFFECTED  NUMBER,
  RUN_AT         TIMESTAMP_NTZ DEFAULT SYSDATE()
);

CREATE OR REPLACE PROCEDURE SP_STAGE_EVENTS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
  -- Reading from the stream inside a DML statement advances its offset when the
  -- statement commits. If the MERGE fails, nothing is consumed and the next run retries.
  MERGE INTO LOGISTICS_DB.STAGING.STG_SHIPMENT_EVENTS t
  USING (
    SELECT
      TRIM(EVENT_ID)                                              AS EVENT_ID,
      TRIM(SHIPMENT_ID)                                           AS SHIPMENT_ID,
      UPPER(TRIM(EVENT_TYPE))                                     AS EVENT_TYPE,
      TRY_TO_TIMESTAMP_NTZ(EVENT_TS, 'YYYY-MM-DD HH24:MI:SS')     AS EVENT_TS,
      NULLIF(TRIM(HUB_ID), '')                                    AS HUB_ID,
      NULLIF(TRIM(DRIVER_ID), '')                                 AS DRIVER_ID,
      NULLIF(TRIM(VEHICLE_ID), '')                                AS VEHICLE_ID,
      NULLIF(UPPER(TRIM(FAILURE_REASON)), '')                     AS FAILURE_REASON,
      _SOURCE_FILE                                                AS SOURCE_FILE,
      _LOADED_AT                                                  AS LOADED_AT
    FROM LOGISTICS_DB.RAW.STRM_RAW_SHIPMENT_EVENTS
    WHERE EVENT_ID IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(EVENT_ID) ORDER BY _LOADED_AT DESC) = 1
  ) s
  ON t.EVENT_ID = s.EVENT_ID
  WHEN NOT MATCHED THEN INSERT
    (EVENT_ID, SHIPMENT_ID, EVENT_TYPE, EVENT_TS, HUB_ID, DRIVER_ID, VEHICLE_ID, FAILURE_REASON, SOURCE_FILE, LOADED_AT)
  VALUES
    (s.EVENT_ID, s.SHIPMENT_ID, s.EVENT_TYPE, s.EVENT_TS, s.HUB_ID, s.DRIVER_ID, s.VEHICLE_ID, s.FAILURE_REASON, s.SOURCE_FILE, s.LOADED_AT);

  LET merged NUMBER := SQLROWCOUNT;
  INSERT INTO LOGISTICS_DB.OPS.PIPELINE_RUN_LOG (PIPELINE_STEP, ROWS_AFFECTED) VALUES ('STAGE_EVENTS', :merged);
  RETURN 'events merged: ' || TO_VARCHAR(merged);
END;
$$;

-- 5. First run (backfill): consumes everything loaded so far ---------------------------------------
CALL LOGISTICS_DB.OPS.SP_STAGE_EVENTS();

SELECT COUNT(*) AS STAGED_EVENTS, MIN(EVENT_TS) AS FIRST_EVENT, MAX(EVENT_TS) AS LAST_EVENT
FROM LOGISTICS_DB.STAGING.STG_SHIPMENT_EVENTS;

-- The raw table holds the duplicates, staging does not:
SELECT
  (SELECT COUNT(*) FROM LOGISTICS_DB.RAW.RAW_SHIPMENT_EVENTS)        AS RAW_ROWS,
  (SELECT COUNT(*) FROM LOGISTICS_DB.STAGING.STG_SHIPMENT_EVENTS)    AS STAGED_ROWS;
