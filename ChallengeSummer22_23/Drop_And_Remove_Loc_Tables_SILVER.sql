-- Databricks notebook source
DROP TABLE IF EXISTS household.hh_demographic_enriched;

DROP TABLE IF EXISTS household.transaction_data_enriched;

DROP TABLE IF EXISTS campaigns.campaign_enriched;

DROP TABLE IF EXISTS coupons.coupon_enriched;

DROP TABLE IF EXISTS products.causal_data_enriched;

DROP TABLE IF EXISTS products.product_causal_data_partitions;

DROP TABLE IF EXISTS household.transaction_causal_data_partitions;

DROP TABLE IF EXISTS household.transaction_info;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC rm -r dbfs:/user/hive/warehouse/campaigns/campaign_enriched

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC rm -r dbfs:/user/hive/warehouse/products/product_causal_data_partitions

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC rm -r dbfs:/user/hive/warehouse/products/causal_data_enriched

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC rm -r dbfs:/user/hive/warehouse/household/transaction_causal_data_partitions

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC rm -r dbfs:/user/hive/warehouse/coupons/coupon_enriched

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC rm -r dbfs:/user/hive/warehouse/household/hh_demographic_enriched

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC rm -r dbfs:/user/hive/warehouse/household/transaction_data_enriched

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC rm -r dbfs:/user/hive/warehouse/household/transaction_info