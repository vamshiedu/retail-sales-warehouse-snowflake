use warehouse compute_wh;
use database snowflake_learning_db;
create schema if not exists silver;
CREATE OR REPLACE TABLE SILVER.STORE_SALES_CLEAN AS 
SELECT 
    ss.ss_sold_date_sk,
    ss.ss_store_sk,
    ss.ss_customer_sk,
    ss.ss_quantity,
    ss.ss_sales_price
FROM BRONZE.STORE_SALES_RAW ss
join SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.DATE_DIM d
on ss.ss_sold_date_sk = d.d_date_sk
where d.d_year between 1998 and 1999
and ss.ss_sales_price is not null 
and ss.ss_quantity > 0;
select min(ss_sold_date_sk) , max(ss_sold_date_sk) from silver.store_sales_clean;
select count(*) from silver.store_sales_clean;
select count(*) from bronze.store_sales_raw;
select count(*) from silver.store_sales_clean
where ss_sales_price is null;
select count(*) from silver.store_sales_clean
where ss_quantity <= 0;

-- Create Customer Dimension table
CREATE OR REPLACE TABLE SILVER.CUSTOMER_DIM(
    customer_sk NUMBER AUTOINCREMENT,
    customer_id NUMBER,
    state STRING,
    start_date DATE,
    end_date DATE,
    current_status STRING
);

INSERT INTO SILVER.CUSTOMER_DIM
(customer_id, state, start_date, end_date, current_status)
SELECT 
    c.c_customer_sk,
    ca.ca_state,
    CURRENT_DATE,
    NULL,
    'Y'
FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER c
JOIN SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER_ADDRESS ca
ON c.c_current_addr_sk = ca.ca_address_sk;

select count(*) from SILVER.CUSTOMER_DIM;

-- Create enriched sales table with transaction dates
CREATE OR REPLACE TABLE SILVER.STORE_SALES_ENRICHED AS
SELECT
    ss.ss_customer_sk,
    ss.ss_store_sk,
    ss.ss_sales_price,
    d.d_date AS transaction_date
FROM SILVER.STORE_SALES_CLEAN ss
JOIN SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.DATE_DIM d
    ON ss.ss_sold_date_sk = d.d_date_sk;

-- Create Fact Sales table
CREATE OR REPLACE TABLE SILVER.FACT_SALES AS
SELECT
    d.customer_sk,
    f.ss_store_sk AS store_id,
    f.ss_sales_price AS sales_price,
    f.transaction_date
FROM SILVER.STORE_SALES_ENRICHED f
JOIN SILVER.CUSTOMER_DIM d
    ON f.ss_customer_sk = d.customer_id
    AND f.transaction_date >= d.start_date
    AND (f.transaction_date < d.end_date OR d.end_date IS NULL);

select count(*) from SILVER.FACT_SALES;