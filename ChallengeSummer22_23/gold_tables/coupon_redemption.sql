-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW filtered_transactions AS
  WITH filtered_transactions AS (
    SELECT *
    FROM  household.transaction_data_enriched
    WHERE trans_date < "2022-09-07"
  )

  SELECT household_key, sum(sales_value_euros) AS total_sales
  FROM filtered_transactions
  GROUP BY household_key
  ORDER BY household_key ASC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW median_basket_view AS
  WITH median_each_basket AS (
    SELECT household_key, BASKET_ID, sum(sales_value_euros) AS median_basket
    FROM household.transaction_data_enriched
    GROUP BY BASKET_ID, household_key
  )

  SELECT household_key, median(median_basket) AS median_basket
  FROM median_each_basket
  GROUP BY household_key

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW transaction_info_view AS
  SELECT household_key, count(STORE_ID) AS total_visits, CAST(avg(sales_value_euros) AS DECIMAL(14,4)) AS avg_price
  FROM household.transaction_data_enriched 
  WHERE sales_value_euros > 0 AND QUANTITY > 0 AND trans_date < "2022-09-07"
  GROUP BY household_key

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW sensitivity_view AS
  WITH filtered_campaigns AS (
    SELECT *
    FROM campaigns.campaign_enriched
    WHERE START_DATE < "2022-09-07"
  ), count_campaigns AS (
    SELECT TI.household_key, COUNT(TI.CAMPAIGN) AS count_campaigns_by_hh_key
    FROM household.transaction_info TI LEFT JOIN filtered_campaigns FC ON FC.household_key = TI.household_key
    GROUP BY TI.household_key
  ), sensitivity_table AS (
  SELECT household_key,
    CASE
      WHEN (count_campaigns_by_hh_key > 0) THEN "Sensible"
      ELSE "Not sensible"
    END AS sensitivity
  FROM count_campaigns
  )

  SELECT *
  FROM sensitivity_table
  ORDER BY household_key ASC

-- COMMAND ----------

-- visits to the stores by month

CREATE EXTERNAL TABLE IF NOT EXISTS household.visits_by_month
USING PARQUET
LOCATION "/user/hive/warehouse/household/visits_by_month"
AS
  SELECT HH.household_key, date_format(trans_date, 'yyyy-MM') AS date, count(STORE_ID) AS visits_by_month
  FROM household.transaction_data_enriched TDE INNER JOIN household.hh_demographic_enriched HH ON TDE.household_key = HH.household_key 
  WHERE sales_value_euros > 0 AND QUANTITY > 0 AND trans_date < "2022-09-07"
  GROUP BY HH.household_key, date
  ORDER BY HH.household_key ASC

-- COMMAND ----------

CREATE EXTERNAL TABLE IF NOT EXISTS household.coupon_redemption
USING PARQUET
LOCATION "/user/hive/warehouse/household/coupon_redemption"
AS
  SELECT HH.household_key, HH.AGE_DESC, HH.MARITAL_STATUS_CODE, HH.INCOME_DESC, HH.HOMEOWNER_DESC, HH.HH_COMP_DESC, HH.HOUSEHOLD_SIZE_DESC, HH.KID_CATEGORY_DESC, SV.sensitivity, FT.total_sales, MBV.median_basket, TFV.total_visits, TFV.avg_price
  FROM household.hh_demographic_enriched HH INNER JOIN filtered_transactions FT ON HH.household_key = FT.household_key 
                                            INNER JOIN median_basket_view MBV ON HH.household_key = MBV.household_key
                                            INNER JOIN transaction_info_view TFV ON HH.household_key = TFV.household_key
                                            INNER JOIN sensitivity_view SV ON HH.household_key = SV.household_key
  ORDER BY HH.household_key ASC