-- Databricks notebook source
CREATE TABLE IF NOT EXISTS products.causal_data_enriched
USING PARQUET
LOCATION "/user/hive/warehouse/products/causal_data_enriched"
AS
  SELECT product_id, store_id, week_no, display, mailer,
    CASE
      WHEN display = 0 THEN 'Not on display'
      WHEN display = 1 THEN 'Store Front'
      WHEN display = 2 THEN 'Store Rear'
      WHEN display = 3 THEN 'Front End Cap'
      WHEN display = 4 THEN 'Mid-Aisle End Cap'
      WHEN display = 5 THEN 'Rear End Cap'
      WHEN display = 6 THEN 'Side-Aisle End Cap'
      WHEN display = 7 THEN 'In-Aisle'
      WHEN display = 9 THEN 'Secondary Location Display'
      WHEN display = 'A' THEN 'In-Shelf'
    END AS display_description,
    CASE
      WHEN mailer = 0 THEN 'Not on ad'
      WHEN mailer = 'A' THEN 'Interior Page Feature'
      WHEN mailer = 'C' THEN 'Interior Page Line Item'
      WHEN mailer = 'D' THEN 'Front Page Feature'
      WHEN mailer = 'F' THEN 'Back Page Feature'
      WHEN mailer = 'H' THEN 'Wrap front feature'
      WHEN mailer = 'J' THEN 'Wrap Interior coupon'
      WHEN mailer = 'L' THEN 'Wrap back feature'
      WHEN mailer = 'P' THEN 'Interior page coupon'
      WHEN mailer = 'X' THEN 'Free on interior page'
      WHEN mailer = 'Z' THEN 'Free on front page'
    END AS mailer_description
  FROM products.causal_data