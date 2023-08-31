-- Databricks notebook source
CREATE EXTERNAL TABLE IF NOT EXISTS household.transaction_causal_data_partitions
USING PARQUET
PARTITIONED BY (trans_date)
LOCATION "/user/hive/warehouse/household/transaction_causal_data_partitions"
AS
  SELECT TDE.*, CDE.display, CDE.mailer, CDE.display_description, CDE.mailer_description 
  FROM household.transaction_data_enriched TDE INNER JOIN products.causal_data_enriched CDE ON TDE.PRODUCT_ID = CDE.product_id AND 
                                                                                               TDE.STORE_ID = CDE.store_id AND 
                                                                                               TDE.WEEK_NO = CDE.week_no
