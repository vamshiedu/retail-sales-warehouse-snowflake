USE  WAREHOUSE COMPUTE_WH;
USE DATABASE SNOWFLAKE_LEARNING_DB;
create schema if not exists gold;
CREATE OR REPLACE TABLE GOLD.SALES_BY_STATE AS
SELECT
    D.state,
    SUM(f.sales_price) AS total_sales
FROM
    SILVER.FACT_SALES f
JOIN
    SILVER.CUSTOMER_DIM d ON f.customer_sk = d.customer_sk
group by d.state;
SELECT * FROM GOLD.SALES_BY_STATE LIMIT 10;
CREATE OR REPLACE TABLE GOLD.SALES_BY_YAER AS
SELECT
    YEAR(transaction_date) AS sales_year,
    SUM(sales_price) AS total_sales
FROM
    SILVER.FACT_SALES 
GROUP BY sales_year;
--Sales by Store every month
CREATE OR REPLACE TABLE GOLD.MONTHLY_SALES AS
SELECT
    DATE_TRUNC('month',transaction_date) as sales_month,
    SUM(sales_price) AS total_sales
FROM SILVER.FACT_SALES
GROUP BY sales_month;

--Top 10 stores by sales
CREATE OR REPLACE TABLE GOLD.TOP_STORES AS
SELECT
     store_id,
     SUM(sales_price) AS total_sales
FROM SILVER.FACT_SALES
GROUP BY store_id
ORDER BY total_sales DESC
LIMIT 10;
SELECT COUNT(*) FROM GOLD.MONTHLY_SALES;
SELECT COUNT(*) FROM GOLD.TOP_STORES;
SELECT COUNT(*) FROM GOLD.SALES_BY_STATE;
SELECT COUNT(*) FROM GOLD.SALES_BY_YAER;
select sum(sales_price) from silver.fact_sales;
select sum(total_sales) from gold.sales_by_state;