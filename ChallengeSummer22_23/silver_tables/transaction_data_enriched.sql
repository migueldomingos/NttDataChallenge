-- Databricks notebook source
CREATE EXTERNAL TABLE IF NOT EXISTS household.transaction_data_enriched
USING PARQUET
LOCATION "/user/hive/warehouse/household/transaction_data_enriched"
AS
  SELECT *, 
    date_format(dateAdd(day, HH.day, '2021-01-01'), 'yyyy-MM-dd') AS trans_date,
    CAST(HH.SALES_VALUE/1.07 AS DECIMAL(10,3)) AS sales_value_euros,
    CAST((HH.SALES_VALUE - HH.RETAIL_DISC + HH.COUPON_MATCH_DISC)/HH.QUANTITY AS DECIMAL(10,3)) AS loyalty_card_price,
    CAST((HH.SALES_VALUE - HH.COUPON_MATCH_DISC)/HH.QUANTITY AS DECIMAL(10,3)) AS nonloyalty_card_price
  FROM household.transaction_data HH