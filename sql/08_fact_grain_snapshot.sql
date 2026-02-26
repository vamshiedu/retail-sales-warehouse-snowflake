use database snowflake_learning_db;
 create or replace table gold.monthly_sales as
select date_trunc('month', transaction_date)
 as sales_month,
       sum(sales_price) as total_sales from silver.fact_sales
group by sales_month;
select * from gold.monthly_sales;