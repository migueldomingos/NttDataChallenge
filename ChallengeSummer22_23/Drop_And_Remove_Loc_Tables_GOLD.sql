-- Databricks notebook source
DROP TABLE IF EXISTS household.coupon_redemption;

DROP TABLE IF EXISTS household.household_spending_partitions;

DROP TABLE IF EXISTS household.visits_by_month;

DROP TABLE IF EXISTS products.top3_sales_by_display;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC rm -r dbfs:/user/hive/warehouse/household/coupon_redemption

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC rm -r dbfs:/user/hive/warehouse/household/household_spending_partitions

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC rm -r dbfs:/user/hive/warehouse/household/visits_by_month

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC rm -r dbfs:/user/hive/warehouse/products/top3_sales_by_display