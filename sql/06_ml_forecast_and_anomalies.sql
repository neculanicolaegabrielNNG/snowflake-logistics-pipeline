/* ============================================================================
   06_ml_forecast_and_anomalies.sql  -  Machine learning with Snowflake ML functions
   Run as LOGISTICS_ENGINEER, after 03_marts.sql.

   Two business questions, both answered in pure SQL (no model code, no data
   leaves Snowflake):

   1. FORECAST   "How many parcels will each hub receive in the next 14 days,
                  and will it be above capacity?"
                  -> SNOWFLAKE.ML.FORECAST (one model, 10 time series)
                  -> honest evaluation with a 14-day hold-out (backtest)

   2. ANOMALIES  "On which days did a hub deliver far more parcels late than
                  it normally does?"
                  -> SNOWFLAKE.ML.ANOMALY_DETECTION, trained on a calm period
                     (first ~7 weeks) and used to score everything after it.
                     With the default data the Black Friday / Christmas peak
                     should light up.

   Note: ML functions need the schema to be owned by (or granted CREATE
   SNOWFLAKE.ML.FORECAST / CREATE SNOWFLAKE.ML.ANOMALY_DETECTION to) the role.
   LOGISTICS_ENGINEER owns the ML schema, so no extra grants are needed.
   ========================================================================== */

USE ROLE LOGISTICS_ENGINEER;
USE WAREHOUSE LOGISTICS_WH;
USE SCHEMA LOGISTICS_DB.ML;

/* ----------------------------------------------------------------------------
   PART 1 - Volume forecast
   ---------------------------------------------------------------------------- */

-- Training input: one row per hub per day (daily frequency, no gaps - FCT_HUB_DAILY
-- is built on a complete hub x date spine, which the forecast function requires).
CREATE OR REPLACE VIEW V_VOLUME_ALL AS
SELECT HUB_CODE                    AS SERIES,
       DATE_DAY::TIMESTAMP_NTZ     AS TS,
       SHIPMENTS_CREATED::FLOAT    AS Y
FROM LOGISTICS_DB.MARTS.FCT_HUB_DAILY;

-- Backtest input: hide the last 14 days from the model.
CREATE OR REPLACE VIEW V_VOLUME_TRAIN_BACKTEST AS
SELECT * FROM V_VOLUME_ALL
WHERE TS <= (SELECT DATEADD('day', -14, MAX(TS)) FROM V_VOLUME_ALL);

-- 1a. Backtest model: trained WITHOUT the last 14 days -----------------------------------
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST VOLUME_MODEL_BACKTEST(
  INPUT_DATA        => TABLE(LOGISTICS_DB.ML.V_VOLUME_TRAIN_BACKTEST),
  SERIES_COLNAME    => 'SERIES',
  TIMESTAMP_COLNAME => 'TS',
  TARGET_COLNAME    => 'Y'
);

CREATE OR REPLACE TABLE FORECAST_BACKTEST AS
SELECT * FROM TABLE(VOLUME_MODEL_BACKTEST!FORECAST(
  FORECASTING_PERIODS => 14,
  CONFIG_OBJECT       => {'prediction_interval': 0.9}
));

-- Compare the forecast with what really happened
CREATE OR REPLACE VIEW V_FORECAST_EVALUATION AS
SELECT
  f.SERIES::VARCHAR                           AS HUB_CODE,
  f.TS::DATE                                  AS DATE_DAY,
  ROUND(f.FORECAST, 1)                        AS FORECAST,
  a.SHIPMENTS_CREATED                         AS ACTUAL,
  ABS(f.FORECAST - a.SHIPMENTS_CREATED)       AS ABS_ERROR,
  (a.SHIPMENTS_CREATED BETWEEN f.LOWER_BOUND AND f.UPPER_BOUND) AS WITHIN_INTERVAL
FROM FORECAST_BACKTEST f
JOIN LOGISTICS_DB.MARTS.FCT_HUB_DAILY a
  ON a.HUB_CODE = f.SERIES::VARCHAR AND a.DATE_DAY = f.TS::DATE;

-- Error per hub. WAPE (weighted absolute % error) is used instead of MAPE because
-- quiet days (Sundays) have tiny actuals that would blow MAPE up.
SELECT
  HUB_CODE,
  SUM(ACTUAL)                                              AS ACTUAL_TOTAL,
  ROUND(SUM(ABS_ERROR) / NULLIF(SUM(ACTUAL), 0) * 100, 1)  AS WAPE_PCT,
  ROUND(AVG(IFF(WITHIN_INTERVAL, 1, 0)) * 100, 0)          AS INTERVAL_COVERAGE_PCT
FROM V_FORECAST_EVALUATION
GROUP BY HUB_CODE
UNION ALL
SELECT 'ALL HUBS', SUM(ACTUAL), ROUND(SUM(ABS_ERROR) / NULLIF(SUM(ACTUAL), 0) * 100, 1),
       ROUND(AVG(IFF(WITHIN_INTERVAL, 1, 0)) * 100, 0)
FROM V_FORECAST_EVALUATION
ORDER BY HUB_CODE;

-- 1b. Production model: trained on ALL data, forecasts the next 14 days ------------------------
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST VOLUME_MODEL(
  INPUT_DATA        => TABLE(LOGISTICS_DB.ML.V_VOLUME_ALL),
  SERIES_COLNAME    => 'SERIES',
  TIMESTAMP_COLNAME => 'TS',
  TARGET_COLNAME    => 'Y'
);

CREATE OR REPLACE TABLE FORECAST_NEXT_14D AS
SELECT * FROM TABLE(VOLUME_MODEL!FORECAST(
  FORECASTING_PERIODS => 14,
  CONFIG_OBJECT       => {'prediction_interval': 0.9}
));

-- Business view: where could we run out of capacity? Uses the UPPER bound of the
-- interval, i.e. a pessimistic but plausible volume.
CREATE OR REPLACE VIEW V_CAPACITY_RISK AS
SELECT
  f.SERIES::VARCHAR                              AS HUB_CODE,
  f.TS::DATE                                     AS DATE_DAY,
  ROUND(f.FORECAST, 0)                           AS FORECAST_SHIPMENTS,
  ROUND(f.UPPER_BOUND, 0)                        AS UPPER_BOUND_SHIPMENTS,
  h.CAPACITY_PER_DAY,
  ROUND(f.UPPER_BOUND / h.CAPACITY_PER_DAY, 3)   AS UPPER_UTILISATION,
  CASE
    WHEN f.UPPER_BOUND > h.CAPACITY_PER_DAY        THEN 'HIGH'
    WHEN f.UPPER_BOUND > 0.85 * h.CAPACITY_PER_DAY THEN 'MEDIUM'
    ELSE 'LOW'
  END                                            AS RISK_LEVEL
FROM FORECAST_NEXT_14D f
JOIN LOGISTICS_DB.MARTS.DIM_HUB h ON h.HUB_CODE = f.SERIES::VARCHAR;

SELECT * FROM V_CAPACITY_RISK WHERE RISK_LEVEL <> 'LOW' ORDER BY UPPER_UTILISATION DESC;


/* ----------------------------------------------------------------------------
   PART 2 - Anomaly detection on late deliveries
   ---------------------------------------------------------------------------- */

-- Target = late deliveries per hub per day. Count-based, so every day has a value
-- (0 on days without deliveries) and the series has no gaps.
-- Train on the first 47 days (calm autumn period), score everything after that.
CREATE OR REPLACE VIEW V_LATE_TRAIN AS
SELECT HUB_CODE AS SERIES, DATE_DAY::TIMESTAMP_NTZ AS TS, LATE_DELIVERIES::FLOAT AS Y
FROM LOGISTICS_DB.MARTS.FCT_HUB_DAILY
WHERE DATE_DAY < (SELECT MIN(DATE_DAY) + 47 FROM LOGISTICS_DB.MARTS.FCT_HUB_DAILY);

CREATE OR REPLACE VIEW V_LATE_SCORE AS
SELECT HUB_CODE AS SERIES, DATE_DAY::TIMESTAMP_NTZ AS TS, LATE_DELIVERIES::FLOAT AS Y
FROM LOGISTICS_DB.MARTS.FCT_HUB_DAILY
WHERE DATE_DAY >= (SELECT MIN(DATE_DAY) + 47 FROM LOGISTICS_DB.MARTS.FCT_HUB_DAILY);

-- Unsupervised model: LABEL_COLNAME must be passed as an empty string when there are no labels.
CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION LATE_DELIVERY_MODEL(
  INPUT_DATA        => TABLE(LOGISTICS_DB.ML.V_LATE_TRAIN),
  SERIES_COLNAME    => 'SERIES',
  TIMESTAMP_COLNAME => 'TS',
  TARGET_COLNAME    => 'Y',
  LABEL_COLNAME     => ''
);

CREATE OR REPLACE TABLE LATE_DELIVERY_ANOMALIES AS
SELECT * FROM TABLE(LATE_DELIVERY_MODEL!DETECT_ANOMALIES(
  INPUT_DATA        => TABLE(LOGISTICS_DB.ML.V_LATE_SCORE),
  SERIES_COLNAME    => 'SERIES',
  TIMESTAMP_COLNAME => 'TS',
  TARGET_COLNAME    => 'Y',
  CONFIG_OBJECT     => {'prediction_interval': 0.95}
));

-- Readable result: which hub, which day, how bad
CREATE OR REPLACE VIEW V_LATE_DELIVERY_ANOMALIES AS
SELECT
  SERIES::VARCHAR          AS HUB_CODE,
  TS::DATE                 AS DATE_DAY,
  Y                        AS LATE_DELIVERIES,
  ROUND(FORECAST, 1)       AS EXPECTED,
  ROUND(UPPER_BOUND, 1)    AS UPPER_BOUND,
  ROUND(DISTANCE, 2)       AS Z_SCORE
FROM LATE_DELIVERY_ANOMALIES
WHERE IS_ANOMALY;

-- Which days were anomalous in the most hubs at once? (a network-wide event, not a local one)
SELECT DATE_DAY, COUNT(*) AS HUBS_AFFECTED, SUM(LATE_DELIVERIES) AS LATE_DELIVERIES
FROM V_LATE_DELIVERY_ANOMALIES
GROUP BY DATE_DAY
ORDER BY HUBS_AFFECTED DESC, DATE_DAY
LIMIT 15;
