-- Databricks notebook source
-- MAGIC %run ./Creating_Databases

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS household.transaction_causal_data_partitions (household_key INTEGER, BASKET_ID LONG, DAY INTEGER, PRODUCT_ID INTEGER, QUANTITY INTEGER, SALES_VALUE DECIMAL(10,3), STORE_ID INTEGER, COUPON_MATCH_DISC DECIMAL(10,3), COUPON_DISC DECIMAL(10,3), RETAIL_DISC DECIMAL(10,3), TRANS_TIME INTEGER, WEEK_NO INTEGER, sales_value_euros DECIMAL(10,3), loyalty_card_price DECIMAL(10,3), nonloyalty_card_price DECIMAL(10,3), display STRING, mailer STRING, display_description STRING, mailer_description STRING, trans_date STRING)
USING PARQUET
LOCATION "/user/hive/warehouse/household/transaction_causal_data_partitions"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS products.product_causal_data_partitions (PRODUCT_ID INTEGER, DEPARTMENT STRING, COMMODITY_DESC STRING, SUB_COMMODITY_DESC STRING, MANUFACTURER INTEGER, BRAND STRING, CURR_SIZE_OF_PRODUCT STRING, store_id INTEGER, display STRING, mailer STRING, display_description STRING, mailer_description STRING, week_no INTEGER)
USING PARQUET
LOCATION "/user/hive/warehouse/products/product_causal_data_partitions"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS campaigns.campaign_enriched (household_key INTEGER, CAMPAIGN INTEGER, DESCRIPTION STRING, START_DAY INTEGER, END_DAY INTEGER, START_DATE STRING, END_DATE STRING, CAMPAIGN_DURATION INTEGER)
USING PARQUET
LOCATION "/user/hive/warehouse/campaigns/campaign_enriched"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS products.causal_data_enriched (product_id INTEGER, store_id INTEGER, week_no INTEGER, display STRING, mailer STRING, display_description STRING, mailer_description STRING)
USING PARQUET
LOCATION "/user/hive/warehouse/products/causal_data_enriched"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS coupons.coupon_enriched (coupon_upc LONG, product_id INTEGER, campaign INTEGER, household_key INTEGER, day INTEGER, trans_date STRING, is_coupon_redeemed BOOLEAN)
USING PARQUET
LOCATION "/user/hive/warehouse/coupons/coupon_enriched"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS household.hh_demographic_enriched (household_key INTEGER, AGE_DESC STRING, MARITAL_STATUS_CODE STRING, INCOME_DESC STRING, HOMEOWNER_DESC STRING, HH_COMP_DESC STRING, HOUSEHOLD_SIZE_DESC STRING, KID_CATEGORY_DESC STRING, marital_status STRING, adult_category_size INTEGER)
USING PARQUET
LOCATION "/user/hive/warehouse/household/hh_demographic_enriched"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS household.transaction_data_enriched (household_key INTEGER, BASKET_ID LONG, DAY INTEGER, PRODUCT_ID INTEGER, QUANTITY INTEGER, SALES_VALUE DECIMAL(10,3), STORE_ID INTEGER, COUPON_MATCH_DISC DECIMAL(10,3), COUPON_DISC DECIMAL(10,3), RETAIL_DISC DECIMAL(10,3), TRANS_TIME INTEGER, WEEK_NO INTEGER, trans_date STRING, sales_value_euros DECIMAL(10,3), loyalty_card_price DECIMAL(10,3), nonloyalty_card_price DECIMAL(10,3))
USING PARQUET
LOCATION "/user/hive/warehouse/household/transaction_data_enriched"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS household.transaction_info (household_key INTEGER, CAMPAIGN INTEGER)
USING PARQUET
LOCATION "/user/hive/warehouse/household/transaction_info"