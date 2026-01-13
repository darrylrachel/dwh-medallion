/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script sets up three schemas within the database: 'bronze', 'silver', and 'gold',
    as well as build the required tables.

WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists.
    All data in the database will be permanently deleted. Proceed with caution
    and ensure you have proper backups before running this script.

docker compose down -v
docker compose up -d
*/



-- --------------------
-- Schemas
-- --------------------
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;


-- --------------------
-- Bronze tables
-- Raw, lightly typed, minimal constraints
-- --------------------
CREATE TABLE IF NOT EXISTS bronze.crm_cust_info (
    cst_id INTEGER,
    cst_key TEXT,
    cst_firstname TEXT,
    cst_lastname TEXT,
    cst_marital_status TEXT,
    cst_gndr TEXT,
    cst_create_date TEXT
);

TRUNCATE TABLE bronze.crm_cust_info;

COPY bronze.crm_cust_info
FROM '/datasets/source_crm/cust_info.csv'
WITH (
  FORMAT csv,
  HEADER true,
  DELIMITER ',',
  NULL '',
  ENCODING 'UTF8'
);

CREATE TABLE IF NOT EXISTS bronze.crm_prd_info (
  prd_id INTEGER,
  prd_key TEXT,
  prd_nm TEXT,
  prd_cost INTEGER,
  prd_line TEXT,
  prd_start_dt TEXT,
  prd_end_dt TEXT
);

TRUNCATE TABLE bronze.crm_prd_info;

COPY bronze.crm_prd_info
FROM '/datasets/source_crm/prd_info.csv'
WITH (
  FORMAT csv,
  HEADER true,
  DELIMITER ',',
  NULL '',
  ENCODING 'UTF8'
);

CREATE TABLE IF NOT EXISTS bronze.crm_sales_details (
    sls_ord_num TEXT,
    sls_prd_key TEXT,
    sls_cust_id INTEGER,
    sls_order_dt TEXT,
    sls_ship_dt TEXT,
    sls_due_dt TEXT,
    sls_sales INTEGER,
    sls_quantity INTEGER,
    sls_price INTEGER
);

TRUNCATE TABLE bronze.crm_sales_details;

COPY bronze.crm_sales_details
FROM '/datasets/source_crm/sales_details.csv'
WITH (
  FORMAT csv,
  HEADER true,
  DELIMITER ',',
  NULL '',
  ENCODING 'UTF8'
);

CREATE TABLE IF NOT EXISTS bronze.erp_cust_az12 (
    cid TEXT,
    bdate TEXT,
    gen TEXT
);

TRUNCATE TABLE bronze.erp_cust_az12;

COPY bronze.erp_cust_az12
FROM '/datasets/source_erp/CUST_AZ12.csv'
WITH (
  FORMAT csv,
  HEADER true,
  DELIMITER ',',
  NULL '',
  ENCODING 'UTF8'
);

CREATE TABLE IF NOT EXISTS bronze.erp_loc_a101 (
    cid TEXT,
    cntry TEXT
);

TRUNCATE TABLE bronze.erp_loc_a101;

COPY bronze.erp_loc_a101
FROM '/datasets/source_erp/loc_a101.csv'
WITH (
  FORMAT csv,
  HEADER true,
  DELIMITER ',',
  NULL '',
  ENCODING 'UTF8'
);

CREATE TABLE IF NOT EXISTS bronze.erp_px_cat_g1v2 (
    id TEXT,
    cat TEXT,
    subcat TEXT,
    maintenance TEXT
);

TRUNCATE TABLE bronze.erp_px_cat_g1v2;

COPY bronze.erp_px_cat_g1v2
FROM '/datasets/source_erp/px_cat_g1v2.csv'
WITH (
  FORMAT csv,
  HEADER true,
  DELIMITER ',',
  NULL '',
  ENCODING 'UTF8'
);


-- --------------------
-- Indexes
-- --------------------
CREATE INDEX IF NOT EXISTS idx_bronze_cust_id
    ON bronze.crm_cust_info (cst_id);
