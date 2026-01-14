-- =============================================================
-- Procedure: bronze.load_bronze
-- Purpose:   Create medallion schemas + bronze tables (if needed),
--            truncate, and load CSVs from /datasets.
-- Start/Stop Docker Container:
--            Stop: docker compose down -v
--            Start: docker compose up -d
-- =============================================================

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
  v_start       timestamptz := clock_timestamp();
  v_step_start  timestamptz;
  v_rows        bigint;
BEGIN
  RAISE NOTICE 'Starting bronze load...';

  -- --------------------
  -- Schemas
  -- --------------------
  CREATE SCHEMA IF NOT EXISTS bronze;
  CREATE SCHEMA IF NOT EXISTS silver;
  CREATE SCHEMA IF NOT EXISTS gold;

  -- --------------------
  -- Tables (Bronze)
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

  CREATE TABLE IF NOT EXISTS bronze.crm_prd_info (
    prd_id INTEGER,
    prd_key TEXT,
    prd_nm TEXT,
    prd_cost INTEGER,
    prd_line TEXT,
    prd_start_dt TEXT,
    prd_end_dt TEXT
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

  CREATE TABLE IF NOT EXISTS bronze.erp_cust_az12 (
      cid TEXT,
      bdate TEXT,
      gen TEXT
  );

  CREATE TABLE IF NOT EXISTS bronze.erp_loc_a101 (
      cid TEXT,
      cntry TEXT
  );

  CREATE TABLE IF NOT EXISTS bronze.erp_px_cat_g1v2 (
      id TEXT,
      cat TEXT,
      subcat TEXT,
      maintenance TEXT
  );

  -- --------------------
  -- Truncate for reload
  -- --------------------
  RAISE NOTICE 'Truncating bronze tables...';
  TRUNCATE TABLE
    bronze.crm_cust_info,
    bronze.crm_prd_info,
    bronze.crm_sales_details,
    bronze.erp_cust_az12,
    bronze.erp_loc_a101,
    bronze.erp_px_cat_g1v2;

  -- --------------------
  -- Load: CRM Cust Info
  -- --------------------
  v_step_start := clock_timestamp();
  EXECUTE format($f$
    COPY bronze.crm_cust_info
    FROM %L
    WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '', ENCODING 'UTF8')
  $f$, '/datasets/source_crm/cust_info.csv');

  SELECT count(*) INTO v_rows FROM bronze.crm_cust_info;
  RAISE NOTICE 'Loaded bronze.crm_cust_info: % rows (%.3f sec)',
    v_rows, EXTRACT(epoch FROM (clock_timestamp() - v_step_start));

  -- --------------------
  -- Load: CRM Product Info
  -- --------------------
  v_step_start := clock_timestamp();
  EXECUTE format($f$
    COPY bronze.crm_prd_info
    FROM %L
    WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '', ENCODING 'UTF8')
  $f$, '/datasets/source_crm/prd_info.csv');

  SELECT count(*) INTO v_rows FROM bronze.crm_prd_info;
  RAISE NOTICE 'Loaded bronze.crm_prd_info: % rows (%.3f sec)',
    v_rows, EXTRACT(epoch FROM (clock_timestamp() - v_step_start));

  -- --------------------
  -- Load: CRM Sales Details
  -- --------------------
  v_step_start := clock_timestamp();
  EXECUTE format($f$
    COPY bronze.crm_sales_details
    FROM %L
    WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '', ENCODING 'UTF8')
  $f$, '/datasets/source_crm/sales_details.csv');

  SELECT count(*) INTO v_rows FROM bronze.crm_sales_details;
  RAISE NOTICE 'Loaded bronze.crm_sales_details: % rows (%.3f sec)',
    v_rows, EXTRACT(epoch FROM (clock_timestamp() - v_step_start));

  -- --------------------
  -- Load: ERP Cust AZ12
  -- --------------------
  v_step_start := clock_timestamp();
  EXECUTE format($f$
    COPY bronze.erp_cust_az12
    FROM %L
    WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '', ENCODING 'UTF8')
  $f$, '/datasets/source_erp/CUST_AZ12.csv');

  SELECT count(*) INTO v_rows FROM bronze.erp_cust_az12;
  RAISE NOTICE 'Loaded bronze.erp_cust_az12: % rows (%.3f sec)',
    v_rows, EXTRACT(epoch FROM (clock_timestamp() - v_step_start));

  -- --------------------
  -- Load: ERP LOC A101
  -- --------------------
  v_step_start := clock_timestamp();
  EXECUTE format($f$
    COPY bronze.erp_loc_a101
    FROM %L
    WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '', ENCODING 'UTF8')
  $f$, '/datasets/source_erp/LOC_A101.csv');  -- adjust if lowercase

  SELECT count(*) INTO v_rows FROM bronze.erp_loc_a101;
  RAISE NOTICE 'Loaded bronze.erp_loc_a101: % rows (%.3f sec)',
    v_rows, EXTRACT(epoch FROM (clock_timestamp() - v_step_start));

  -- --------------------
  -- Load: ERP PX CAT G1V2
  -- --------------------
  v_step_start := clock_timestamp();
  EXECUTE format($f$
    COPY bronze.erp_px_cat_g1v2
    FROM %L
    WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '', ENCODING 'UTF8')
  $f$, '/datasets/source_erp/PX_CAT_G1V2.csv'); -- adjust if lowercase

  SELECT count(*) INTO v_rows FROM bronze.erp_px_cat_g1v2;
  RAISE NOTICE 'Loaded bronze.erp_px_cat_g1v2: % rows (%.3f sec)',
    v_rows, EXTRACT(epoch FROM (clock_timestamp() - v_step_start));

  -- --------------------
  -- Indexes
  -- --------------------
  CREATE INDEX IF NOT EXISTS idx_bronze_cust_id
      ON bronze.crm_cust_info (cst_id);

  RAISE NOTICE 'Bronze load complete. Total time: %.3f sec',
    EXTRACT(epoch FROM (clock_timestamp() - v_start));

END;
$$;

-- Run it:
CALL bronze.load_bronze();

