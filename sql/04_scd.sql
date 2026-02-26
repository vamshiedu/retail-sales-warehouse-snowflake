DESC TABLE SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER;
DESC TABLE SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER_ADDRESS;

USE WAREHOUSE COMPUTE_WH;

USE DATABASE SNOWFLAKE_LEARNING_DB;

CREATE OR REPLACE TABLE SILVER.CUSTOMER_DIM(
    customer_sk NUMBER AUTOINCREMENT,
    customer_id NUMBER,
    state STRING,
    start_date DATE,
    end_date DATE,
    is_current_status STRING
);

INSERT INTO SILVER.CUSTOMER_DIM
(customer_id, state, start_date,end_date,is_current_status)
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
select * from SILVER.CUSTOMER_DIM limit 5;
---craete a small stage
create or replace table silver.customer_stage as select
customer_id, state from  SILVER.CUSTOMER_DIM WHERE
is_current_status = 'Y';

Alter table silver.customer_dim rename column is_current_status to current_status;
update silver.customer_stage set state = 'TX'
where customer_id = 79911230;
--implementing  Type 2 using merge statement
MERGE INTO silver.customer_dim TARGET
USING silver.customer_stage SOURCE
ON TARGET.customer_id = SOURCE.customer_id
AND TARGET.current_status  = 'Y'
WHEN MATCHED and TARGET.state <> SOURCE.state THEN
    UPDATE SET target.end_date = CURRENT_DATE,
    target.current_status = 'N'
WHEN NOT MATCHED THEN
    INSERT (customer_id, state, start_date, end_date, current_status)
    VALUES (SOURCE.customer_id, SOURCE.state, CURRENT_DATE, NULL, 'Y');

    desc table silver.customer_dim;
    ---validate sdc behavior
SELECT * FROM silver.customer_dim
WHERE customer_id = 79911230
ORDER BY start_date;