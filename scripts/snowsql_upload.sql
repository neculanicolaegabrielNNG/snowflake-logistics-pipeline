-- ============================================================================
-- scripts/snowsql_upload.sql
-- Uploads the generated CSV files to the internal stage.
--
-- PUT is a client-side command: it works from SnowSQL, the Snowflake CLI or a
-- driver, but NOT from a Snowsight worksheet.
--
-- Run from the repository root, after `python3 data_generator/generate_data.py`:
--
--     snowsql -a <account_identifier> -u <user> -f scripts/snowsql_upload.sql
--
-- The paths below are relative to the folder you run snowsql from. The PUT docs only show
-- absolute paths, so if PUT says "file not found", replace them with absolute ones.
-- Windows: use forward slashes or an absolute path, e.g. file://C:/path/to/repo/data/raw/hubs.csv
-- Alternative: Snowflake CLI ->  snow stage copy data/raw/hubs.csv @LOGISTICS_DB.RAW.STG_LOGISTICS/hubs/
-- ============================================================================

USE ROLE LOGISTICS_ENGINEER;
USE WAREHOUSE LOGISTICS_WH;
USE SCHEMA LOGISTICS_DB.RAW;

PUT file://data/raw/hubs.csv                  @STG_LOGISTICS/hubs/             AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
PUT file://data/raw/vehicles.csv              @STG_LOGISTICS/vehicles/         AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
PUT file://data/raw/drivers.csv               @STG_LOGISTICS/drivers/          AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
PUT file://data/raw/customers.csv             @STG_LOGISTICS/customers/        AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
PUT file://data/raw/shipments/*.csv           @STG_LOGISTICS/shipments/        AUTO_COMPRESS=TRUE OVERWRITE=TRUE PARALLEL=8;
PUT file://data/raw/shipment_events/*.csv     @STG_LOGISTICS/shipment_events/  AUTO_COMPRESS=TRUE OVERWRITE=TRUE PARALLEL=8;

LIST @STG_LOGISTICS;
