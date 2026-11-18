/* ============================================================================
   05_data_quality_checks.sql  -  Data quality as code
   Run as LOGISTICS_ENGINEER, after 03_marts.sql.

   OPS.V_DQ_CHECKS returns one row per check:
     SEVERITY = 'ERROR'  -> must be 0, otherwise STATUS = 'FAIL'
     SEVERITY = 'INFO'   -> expected to be > 0 (known, handled data issues)

   Query it after every load:   SELECT * FROM LOGISTICS_DB.OPS.V_DQ_CHECKS;
   ========================================================================== */

USE ROLE LOGISTICS_ENGINEER;
USE WAREHOUSE LOGISTICS_WH;
USE SCHEMA LOGISTICS_DB.OPS;

CREATE OR REPLACE VIEW V_DQ_CHECKS AS
WITH checks AS (

  -- ---- staging: events ---------------------------------------------------------
  SELECT 'stg_events: duplicate event_id' AS CHECK_NAME, 'ERROR' AS SEVERITY, COUNT(*) AS FAILING_ROWS
  FROM (SELECT EVENT_ID FROM LOGISTICS_DB.STAGING.STG_SHIPMENT_EVENTS GROUP BY EVENT_ID HAVING COUNT(*) > 1)

  UNION ALL
  SELECT 'stg_events: unparseable timestamp', 'ERROR', COUNT(*)
  FROM LOGISTICS_DB.STAGING.STG_SHIPMENT_EVENTS WHERE EVENT_TS IS NULL

  UNION ALL
  SELECT 'stg_events: unknown event_type', 'ERROR', COUNT(*)
  FROM LOGISTICS_DB.STAGING.STG_SHIPMENT_EVENTS
  WHERE EVENT_TYPE IS NULL   -- NULL NOT IN (...) is NULL, so a missing type would otherwise slip through
     OR EVENT_TYPE NOT IN ('CREATED', 'PICKED_UP', 'HUB_INBOUND', 'HUB_OUTBOUND', 'OUT_FOR_DELIVERY',
                           'ATTEMPT_FAILED', 'DELIVERED', 'RETURNED_TO_SENDER')

  UNION ALL
  SELECT 'stg_events: event without a shipment', 'ERROR', COUNT(*)
  FROM LOGISTICS_DB.STAGING.STG_SHIPMENT_EVENTS e
  LEFT JOIN LOGISTICS_DB.STAGING.STG_SHIPMENTS s ON s.SHIPMENT_ID = e.SHIPMENT_ID
  WHERE s.SHIPMENT_ID IS NULL

  UNION ALL
  SELECT 'stg_events: failure_reason must be set on ATTEMPT_FAILED events and only on them', 'ERROR', COUNT(*)
  FROM LOGISTICS_DB.STAGING.STG_SHIPMENT_EVENTS
  WHERE COALESCE(EVENT_TYPE = 'ATTEMPT_FAILED', FALSE) <> (FAILURE_REASON IS NOT NULL)

  -- ---- staging: shipments ---------------------------------------------------------
  UNION ALL
  SELECT 'stg_shipments: weight missing or not positive', 'ERROR', COUNT(*)
  FROM LOGISTICS_DB.STAGING.STG_SHIPMENTS WHERE WEIGHT_KG IS NULL OR WEIGHT_KG <= 0

  UNION ALL
  SELECT 'stg_shipments: unknown origin/destination hub', 'ERROR', COUNT(*)
  FROM LOGISTICS_DB.STAGING.STG_SHIPMENTS s
  WHERE s.ORIGIN_HUB_ID NOT IN (SELECT HUB_ID FROM LOGISTICS_DB.STAGING.STG_HUBS)
     OR s.DESTINATION_HUB_ID NOT IN (SELECT HUB_ID FROM LOGISTICS_DB.STAGING.STG_HUBS)

  UNION ALL
  SELECT 'stg_shipments: unknown customer', 'ERROR', COUNT(*)
  FROM LOGISTICS_DB.STAGING.STG_SHIPMENTS
  WHERE CUSTOMER_ID NOT IN (SELECT CUSTOMER_ID FROM LOGISTICS_DB.STAGING.STG_CUSTOMERS)

  UNION ALL
  SELECT 'stg_shipments: promised date before creation date', 'ERROR', COUNT(*)
  FROM LOGISTICS_DB.STAGING.STG_SHIPMENTS WHERE PROMISED_DELIVERY_DATE < CREATED_AT::DATE

  -- ---- marts --------------------------------------------------------------------------
  UNION ALL
  SELECT 'fct_shipment: row count differs from staging', 'ERROR',
         ABS((SELECT COUNT(*) FROM LOGISTICS_DB.MARTS.FCT_SHIPMENT)
           - (SELECT COUNT(*) FROM LOGISTICS_DB.STAGING.STG_SHIPMENTS))

  UNION ALL
  SELECT 'fct_shipment: delivered before created', 'ERROR', COUNT(*)
  FROM LOGISTICS_DB.MARTS.FCT_SHIPMENT WHERE DELIVERED_AT < CREATED_AT

  UNION ALL
  SELECT 'fct_shipment: both delivered and returned', 'ERROR', COUNT(*)
  FROM LOGISTICS_DB.MARTS.FCT_SHIPMENT WHERE DELIVERED_AT IS NOT NULL AND RETURNED_AT IS NOT NULL

  UNION ALL
  SELECT 'fct_shipment: not picked up 3+ days after creation', 'ERROR', COUNT(*)
  FROM LOGISTICS_DB.MARTS.FCT_SHIPMENT WHERE PICKED_UP_AT IS NULL AND DELIVERY_ATTEMPTS = 0 AND STATUS = 'IN_TRANSIT'
    AND CREATED_AT < DATEADD('day', -3, (SELECT MAX(CREATED_AT) FROM LOGISTICS_DB.MARTS.FCT_SHIPMENT))

  UNION ALL
  SELECT 'fct_hub_daily: shipments created does not match fct_shipment', 'ERROR',
         ABS((SELECT SUM(SHIPMENTS_CREATED) FROM LOGISTICS_DB.MARTS.FCT_HUB_DAILY)
           - (SELECT COUNT(*) FROM LOGISTICS_DB.MARTS.FCT_SHIPMENT))

  -- ---- known, handled data issues (informational) ----------------------------------------
  UNION ALL
  SELECT 'info: duplicate raw events removed in staging', 'INFO',
         (SELECT COUNT(*) FROM LOGISTICS_DB.RAW.RAW_SHIPMENT_EVENTS)
       - (SELECT COUNT(*) FROM LOGISTICS_DB.STAGING.STG_SHIPMENT_EVENTS)

  UNION ALL
  SELECT 'info: late-arriving events (file date after event date)', 'INFO', COUNT(*)
  FROM LOGISTICS_DB.STAGING.STG_SHIPMENT_EVENTS
  WHERE TRY_TO_DATE(REGEXP_SUBSTR(SOURCE_FILE, '[0-9]{4}-[0-9]{2}-[0-9]{2}')) > EVENT_TS::DATE

  UNION ALL
  SELECT 'info: shipments still in transit at the end of the extract', 'INFO', COUNT(*)
  FROM LOGISTICS_DB.MARTS.FCT_SHIPMENT WHERE STATUS = 'IN_TRANSIT'
)
SELECT
  CHECK_NAME,
  SEVERITY,
  FAILING_ROWS,
  CASE
    WHEN SEVERITY = 'INFO'    THEN 'INFO'
    WHEN FAILING_ROWS = 0     THEN 'PASS'
    ELSE 'FAIL'
  END AS STATUS
FROM checks;

-- Run it ---------------------------------------------------------------------------------
-- Alphabetical order of STATUS is FAIL, INFO, PASS, so failures are listed first.
SELECT * FROM V_DQ_CHECKS ORDER BY STATUS, SEVERITY, CHECK_NAME;
