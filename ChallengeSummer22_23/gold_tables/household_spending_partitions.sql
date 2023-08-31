-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW total_coupon_spending AS(
  SELECT T.household_key, date_format(T.TRANS_DATE, 'yyyy-MM') AS date, SUM(T.SALES_VALUE) AS VALUE FROM coupons.coupon_enriched C
  INNER JOIN household.transaction_data_enriched T ON T.household_key = C.household_key AND T.PRODUCT_ID = C.product_id AND T.trans_date = C.trans_date AND C.is_coupon_redeemed = 'True'
  GROUP BY T.household_key, date
)


-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW total_spending AS (
  SELECT household_key, date_format(trans_date, 'yyyy-MM')  AS date, SUM(SALES_VALUE) AS VALUE
  FROM household.transaction_data_enriched
  GROUP BY household_key, date
)



-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW effort_rate AS (
    SELECT T.household_key, T.date, CAST((T.VALUE/((CASE WHEN H.INCOME_DESC = 'Under 15K' THEN 8400 WHEN H.INCOME_DESC = '15-24K' THEN 15000 WHEN H.INCOME_DESC = '25-34K' THEN 25000 WHEN H.INCOME_DESC = '35-49K' THEN 35000 WHEN H.INCOME_DESC = '50-74K' THEN 50000 WHEN H.INCOME_DESC = '75-99K' THEN 75000 WHEN H.INCOME_DESC = '100-124K' THEN 100000 WHEN H.INCOME_DESC = '125-149K' THEN 125000 WHEN H.INCOME_DESC = '150-174K' THEN 150000 WHEN H.INCOME_DESC = '175-199K' THEN 175000 WHEN H.INCOME_DESC = '200-249K' THEN 200000 WHEN H.INCOME_DESC = '250K+' THEN 250000 ELSE 0 END)/12)) AS DECIMAL(20,3)) AS effort_rate FROM total_spending T
    INNER JOIN household.hh_demographic_enriched H ON H.household_key = T.household_key
)



-- COMMAND ----------

CREATE EXTERNAL TABLE IF NOT EXISTS household.household_spending_partitions
USING PARQUET
LOCATION "/user/hive/warehouse/household/household_spending_partitions"
AS
  SELECT H.household_key, H.age_desc, H.marital_status_code, H.INCOME_DESC, H.HOMEOWNER_DESC, H.HH_COMP_DESC, H.HOUSEHOLD_SIZE_DESC, H.KID_CATEGORY_DESC, T.date AS year_month, T.VALUE AS total_spending, C.VALUE AS total_coupon_spending, E.effort_rate AS effort_rate  
  FROM household.hh_demographic_enriched H
  INNER JOIN total_spending T ON H.household_key = T.household_key 
  INNER JOIN total_coupon_spending C ON H.household_key = C.household_key AND C.date = T.date
  INNER JOIN effort_rate E ON H.household_key = E.household_key AND E.date = T.date
  ORDER BY H.household_key, year_month
