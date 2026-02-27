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
