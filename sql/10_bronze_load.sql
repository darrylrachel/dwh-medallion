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
