/* ============================================================================
   04_streams_tasks.sql  -  Orchestration with a task DAG
   Run as LOGISTICS_ENGINEER, after 03_marts.sql.

   RAW (COPY INTO) --stream--> T_STAGE_EVENTS --AFTER--> T_BUILD_MARTS

   * T_STAGE_EVENTS is the root task. It wakes up every 5 minutes but only
     starts the warehouse when the stream actually has new rows (WHEN clause),
     so an idle pipeline costs (almost) nothing.
   * T_BUILD_MARTS is a child task: it runs only after the root succeeded.

   COST WARNING: a trial account has limited credits. Resume the tasks only for
   the demo and SUSPEND them afterwards (last statement of this file).
   ========================================================================== */

USE ROLE LOGISTICS_ENGINEER;
USE WAREHOUSE LOGISTICS_WH;
USE SCHEMA LOGISTICS_DB.OPS;

-- Re-run safety: a task graph can only be changed while its root task is suspended,
-- so stop it first (no-op the first time, because the tasks do not exist yet).
ALTER TASK IF EXISTS T_STAGE_EVENTS SUSPEND;

CREATE OR REPLACE TASK T_STAGE_EVENTS
  WAREHOUSE = LOGISTICS_WH
  SCHEDULE  = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('LOGISTICS_DB.RAW.STRM_RAW_SHIPMENT_EVENTS')
AS
  CALL LOGISTICS_DB.OPS.SP_STAGE_EVENTS();

CREATE OR REPLACE TASK T_BUILD_MARTS
  WAREHOUSE = LOGISTICS_WH
  AFTER T_STAGE_EVENTS
AS
  CALL LOGISTICS_DB.OPS.SP_BUILD_MARTS();

-- Tasks are created SUSPENDED. Resume children first, the root last. ---------------------
ALTER TASK T_BUILD_MARTS  RESUME;
ALTER TASK T_STAGE_EVENTS RESUME;

SHOW TASKS IN SCHEMA LOGISTICS_DB.OPS;

-- ---------------------------------------------------------------------------------------
-- DEMO: simulate new data arriving
--   1. Load more files (widen the PATTERN of the COPY INTO in 01_raw_tables_and_load.sql,
--      e.g. to '.*shipment_events_2025-11-.*').
--   2. The stream now has rows:
-- ---------------------------------------------------------------------------------------
-- SELECT SYSTEM$STREAM_HAS_DATA('LOGISTICS_DB.RAW.STRM_RAW_SHIPMENT_EVENTS');
--
--   3. Wait up to 5 minutes, or trigger the DAG now:
-- EXECUTE TASK T_STAGE_EVENTS;
--
--   4. Watch it run:
-- SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, ERROR_MESSAGE
-- FROM TABLE(LOGISTICS_DB.INFORMATION_SCHEMA.TASK_HISTORY(RESULT_LIMIT => 10))
-- ORDER BY SCHEDULED_TIME DESC;
--
--   5. And see what each step did:
-- SELECT * FROM PIPELINE_RUN_LOG ORDER BY RUN_AT DESC LIMIT 10;

-- Always stop the schedule when you are done ----------------------------------------------
-- ALTER TASK T_STAGE_EVENTS SUSPEND;
-- ALTER TASK T_BUILD_MARTS  SUSPEND;
