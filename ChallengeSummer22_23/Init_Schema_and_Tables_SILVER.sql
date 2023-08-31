-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC Use this after initiating the BRONZE tables.
-- MAGIC Then, when you want to have access to the tables when the clutcher gets detached, use the notebook Load_Silver_Tables

-- COMMAND ----------

-- MAGIC %run ./silver_tables/campaign_enriched

-- COMMAND ----------

-- MAGIC %run ./silver_tables/causal_data_enriched

-- COMMAND ----------

-- MAGIC %run ./silver_tables/coupon_enriched

-- COMMAND ----------

-- MAGIC %run ./silver_tables/hh_demographic_enriched

-- COMMAND ----------

-- MAGIC %run ./silver_tables/product_causal_data_partitions

-- COMMAND ----------

-- MAGIC %run ./silver_tables/transaction_data_enriched

-- COMMAND ----------

-- MAGIC %run ./silver_tables/transaction_causal_data_partitions

-- COMMAND ----------

-- MAGIC %run ./silver_tables/transaction_info