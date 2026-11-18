/* ============================================================================
   07_analytics_queries.sql  -  Example business questions on the MARTS layer
   Run any query on its own. All of them read only from LOGISTICS_DB.MARTS.
   ========================================================================== */

USE ROLE LOGISTICS_ENGINEER;
USE WAREHOUSE LOGISTICS_WH;
USE SCHEMA LOGISTICS_DB.MARTS;

-- 1. Network KPIs by month (delivered parcels only) ---------------------------------------
SELECT
  DATE_TRUNC('month', CREATED_DATE)                    AS MONTH,
  COUNT(*)                                             AS SHIPMENTS,
  ROUND(AVG(IFF(IS_ON_TIME, 1, 0)) * 100, 1)           AS ON_TIME_PCT,
  ROUND(AVG(IFF(IS_FIRST_ATTEMPT_DELIVERY, 1, 0)) * 100, 1) AS FIRST_ATTEMPT_PCT,
  ROUND(AVG(TRANSIT_HOURS), 1)                         AS AVG_TRANSIT_HOURS
FROM FCT_SHIPMENT
WHERE STATUS = 'DELIVERED'
GROUP BY 1
ORDER BY 1;

-- 2. Express vs standard: is the premium service really faster? ----------------------------
SELECT
  SERVICE_LEVEL,
  COUNT(*)                                   AS SHIPMENTS,
  ROUND(AVG(TRANSIT_HOURS), 1)               AS AVG_TRANSIT_HOURS,
  ROUND(AVG(IFF(IS_ON_TIME, 1, 0)) * 100, 1) AS ON_TIME_PCT
FROM FCT_SHIPMENT
WHERE STATUS = 'DELIVERED'
GROUP BY 1;

-- 3. Why do deliveries fail? Cash-on-delivery vs prepaid ------------------------------------
SELECT
  IFF(IS_COD, 'COD (ramburs)', 'Prepaid')                      AS PAYMENT,
  COUNT(*)                                                     AS SHIPMENTS,
  ROUND(AVG(IFF(FAILED_ATTEMPTS > 0, 1, 0)) * 100, 1)          AS ANY_FAILED_ATTEMPT_PCT,
  ROUND(COUNT_IF(STATUS = 'RETURNED') / COUNT(*) * 100, 2)     AS RETURNED_PCT
FROM FCT_SHIPMENT
WHERE STATUS IN ('DELIVERED', 'RETURNED')
GROUP BY 1;

SELECT LAST_FAILURE_REASON, COUNT(*) AS PARCELS
FROM FCT_SHIPMENT
WHERE LAST_FAILURE_REASON IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC;

-- 4. Hub scorecard: volume, punctuality and how close to capacity each hub runs ---------------
SELECT
  HUB_CODE,
  SUM(SHIPMENTS_CREATED)                                          AS SHIPMENTS_CREATED,
  ROUND(AVG(CAPACITY_UTILISATION) * 100, 1)                       AS AVG_UTILISATION_PCT,
  ROUND(MAX(CAPACITY_UTILISATION) * 100, 1)                       AS PEAK_UTILISATION_PCT,
  COUNT_IF(CAPACITY_UTILISATION > 1)                              AS DAYS_OVER_CAPACITY,
  ROUND(SUM(DELIVERED_ON_TIME) / NULLIF(SUM(SHIPMENTS_DELIVERED), 0) * 100, 1) AS ON_TIME_PCT
FROM FCT_HUB_DAILY
GROUP BY HUB_CODE
ORDER BY DAYS_OVER_CAPACITY DESC, SHIPMENTS_CREATED DESC;

-- 5. Does punctuality drop when a hub runs close to or over capacity? --------------------
SELECT
  CASE
    WHEN CAPACITY_UTILISATION < 0.7 THEN '1) < 70 %'
    WHEN CAPACITY_UTILISATION < 0.9 THEN '2) 70-90 %'
    WHEN CAPACITY_UTILISATION <= 1  THEN '3) 90-100 %'
    ELSE                                 '4) over capacity'
  END                                                              AS UTILISATION_BAND,
  COUNT(*)                                                         AS HUB_DAYS,
  ROUND(SUM(DELIVERED_ON_TIME) / NULLIF(SUM(SHIPMENTS_DELIVERED), 0) * 100, 1) AS ON_TIME_PCT
FROM FCT_HUB_DAILY
WHERE SHIPMENTS_CREATED > 0
GROUP BY 1
ORDER BY 1;
-- Note: delivery punctuality of a given day reflects load from the days before it, so this
-- is a first-order view. A lagged join (utilisation 1-3 days earlier) would sharpen it.

-- 6. Black Friday week, day by day ---------------------------------------------------------------
SELECT
  CREATED_DATE,
  COUNT(*)                                   AS SHIPMENTS,
  ROUND(AVG(IFF(IS_ON_TIME, 1, 0)) * 100, 1) AS ON_TIME_PCT,
  ROUND(AVG(DAYS_LATE), 2)                   AS AVG_DAYS_LATE
FROM FCT_SHIPMENT
WHERE STATUS = 'DELIVERED'
  AND CREATED_DATE BETWEEN '2025-11-21' AND '2025-12-07'
GROUP BY 1
ORDER BY 1;

-- 7. Biggest customers and their service quality ------------------------------------------------------
SELECT
  c.CUSTOMER_NAME, c.SEGMENT,
  COUNT(*)                                   AS SHIPMENTS,
  ROUND(AVG(IFF(f.IS_ON_TIME, 1, 0)) * 100, 1) AS ON_TIME_PCT
FROM FCT_SHIPMENT f
JOIN DIM_CUSTOMER c ON c.CUSTOMER_ID = f.CUSTOMER_ID
WHERE f.STATUS = 'DELIVERED'
GROUP BY 1, 2
ORDER BY SHIPMENTS DESC
LIMIT 10;

-- 8. Driver productivity (last delivery driver per parcel) ---------------------------------------------------
SELECT
  d.DRIVER_ID, d.FULL_NAME, d.HUB_CODE,
  COUNT(*)                                          AS PARCELS_DELIVERED,
  ROUND(AVG(IFF(f.IS_FIRST_ATTEMPT_DELIVERY, 1, 0)) * 100, 1) AS FIRST_ATTEMPT_PCT
FROM FCT_SHIPMENT f
JOIN DIM_DRIVER d ON d.DRIVER_ID = f.LAST_DRIVER_ID
WHERE f.STATUS = 'DELIVERED'
GROUP BY 1, 2, 3
ORDER BY PARCELS_DELIVERED DESC
LIMIT 10;
