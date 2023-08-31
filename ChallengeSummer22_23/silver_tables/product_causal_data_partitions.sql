-- Databricks notebook source
CREATE EXTERNAL TABLE IF NOT EXISTS products.product_causal_data_partitions
  USING PARQUET
  PARTITIONED BY (week_no)
  LOCATION "/user/hive/warehouse/products/product_causal_data_partitions"
  AS 
    SELECT P.*, CDE.store_id, CDE.week_no, 
      CDE.display, CDE.mailer, CDE.display_description, CDE.mailer_description 
    FROM products.causal_data_enriched CDE INNER JOIN products.product P ON P.product_id = CDE.product_id
