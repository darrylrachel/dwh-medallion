COPY bronze.crm_cust_info
FROM 'C:\Users\rache\Code\data\projects\dwh-medallion\datasets\source_crm\cust_info.csv'
WITH (
  FORMAT csv,
  HEADER true,
  DELIMITER ',',
  NULL '',
  ENCODING 'UTF8'
);
