-- Databricks notebook source
-- MAGIC %run ./Creating_Databases

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS household.coupon_redemption (household_key INTEGER, AGE_DESC STRING, MARITAL_STATUS_CODE STRING, INCOME_DESC STRING, HOMEOWNER_DESC STRING, HH_COMP_DESC STRING, HOUSEHOLD_SIZE_DESC STRING, KID_CATEGORY_DESC STRING, sensitivity STRING, total_sales DECIMAL(20,3), median_basket DOUBLE, total_visits LONG, avg_price DECIMAL(14,4))
USING PARQUET
LOCATION "/user/hive/warehouse/household/coupon_redemption"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS household.household_spending_partitions (household_key INTEGER, age_desc STRING, marital_status_code STRING, INCOME_DESC STRING, HOMEOWNER_DESC STRING, HH_COMP_DESC STRING, HOUSEHOLD_SIZE_DESC STRING, KID_CATEGORY_DESC STRING, year_month STRING, total_spending DECIMAL(20,3), total_coupon_spending DECIMAL(20,3), effort_rate DECIMAL(20,3))
USING PARQUET
LOCATION "/user/hive/warehouse/household/household_spending_partitions"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS products.top3_sales_by_display (product_id INTEGER, display_description STRING, num_sales LONG)
USING PARQUET
LOCATION "/user/hive/warehouse/products/top3_sales_by_display"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS household.query_months_income (income_desc STRING, number_of_rows LONG, min_ts DECIMAL(20,3), max_ts DECIMAL(20,3), avg_ts DECIMAL(10,3), median_ts DECIMAL(10,3), stddev_ts DECIMAL(10,3), min_tcs DECIMAL(20,3), max_tcs DECIMAL(20,3), avg_tcs DECIMAL(10,3), median_tcs DECIMAL(10,3), stddev_tcs DECIMAL(10,3), min_er DECIMAL(20,3), max_er DECIMAL(20,3), avg_er DECIMAL(10,3), median_er DECIMAL(10,3), stddev_er DECIMAL(10,3))
USING PARQUET
LOCATION "/user/hive/warehouse/household/query_months_income"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS household.query_months_age (age_desc STRING, number_of_rows LONG, min_ts DECIMAL(20,3), max_ts DECIMAL(20,3), avg_ts DECIMAL(10,3), median_ts DECIMAL(10,3), stddev_ts DECIMAL(10,3), min_tcs DECIMAL(20,3), max_tcs DECIMAL(20,3), avg_tcs DECIMAL(10,3), median_tcs DECIMAL(10,3), stddev_tcs DECIMAL(10,3), min_er DECIMAL(20,3), max_er DECIMAL(20,3), avg_er DECIMAL(10,3), median_er DECIMAL(10,3), stddev_er DECIMAL(10,3))
USING PARQUET
LOCATION "/user/hive/warehouse/household/query_months_age"