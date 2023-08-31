-- Databricks notebook source
CREATE EXTERNAL TABLE IF NOT EXISTS campaigns.campaign_enriched
USING PARQUET
LOCATION "/user/hive/warehouse/campaigns/campaign_enriched"
AS
  SELECT T.household_key, D.CAMPAIGN, D.DESCRIPTION, D.START_DAY, D.END_DAY, 
    date_format(dateAdd(day, START_DAY, '2021-01-01'), 'yyyy-MM-dd') AS START_DATE, 
    date_format(dateAdd(day, END_DAY, '2021-01-01'), 'yyyy-MM-dd') AS END_DATE,
    (END_DAY - START_DAY) AS CAMPAIGN_DURATION
  FROM campaigns.campaign_desc D INNER JOIN campaigns.campaign_table T ON D.CAMPAIGN = T.CAMPAIGN