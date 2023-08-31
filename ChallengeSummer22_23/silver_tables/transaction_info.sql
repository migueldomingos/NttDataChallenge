-- Databricks notebook source
CREATE TABLE IF NOT EXISTS household.transaction_info
USING PARQUET 
LOCATION "/user/hive/warehouse/household/transaction_info"
AS
  SELECT DISTINCT HH.household_key, CE.CAMPAIGN
  FROM household.hh_demographic_enriched HH LEFT JOIN campaigns.campaign_enriched CE ON HH.household_key = CE.household_key
  ORDER BY HH.household_key ASC