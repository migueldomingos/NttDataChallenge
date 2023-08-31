-- Databricks notebook source
CREATE TABLE IF NOT EXISTS household.hh_demographic_enriched 
  USING PARQUET 
  LOCATION "/user/hive/warehouse/household/hh_demographic_enriched"
  AS
    SELECT *, 
      CASE
        WHEN MARITAL_STATUS_CODE = "A" THEN "Married"
        WHEN MARITAL_STATUS_CODE = "B" THEN "Single"
        WHEN MARITAL_STATUS_CODE = "U" THEN "Unknown"
      END AS marital_status,
      CASE
        WHEN HH_COMP_DESC = "2 Adults No Kids" OR HH_COMP_DESC = "2 Adults Kids" then 2
        WHEN HH_COMP_DESC = "Single Female" OR HH_COMP_DESC = "Single Male"  OR HH_COMP_DESC = "1 Adult Kids" THEN 1
        WHEN HH_COMP_DESC = "Unknown" THEN 0
      END AS adult_category_size
    FROM household.hh_demographic