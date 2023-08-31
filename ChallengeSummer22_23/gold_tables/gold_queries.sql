-- Databricks notebook source


CREATE OR REPLACE TEMP VIEW number_rows_income AS(

    SELECT income_desc,CASE WHEN H.INCOME_DESC = 'Under 15K' THEN 8400 WHEN H.INCOME_DESC = '15-24K' THEN 15000 WHEN H.INCOME_DESC = '25-34K' THEN 25000 WHEN H.INCOME_DESC = '35-49K' THEN 35000 WHEN H.INCOME_DESC = '50-74K' THEN 50000 WHEN H.INCOME_DESC = '75-99K' THEN 75000 WHEN H.INCOME_DESC = '100-124K' THEN 100000 WHEN H.INCOME_DESC = '125-149K' THEN 125000 WHEN H.INCOME_DESC = '150-174K' THEN 150000 WHEN H.INCOME_DESC = '175-199K' THEN 175000 WHEN H.INCOME_DESC = '200-249K' THEN 200000 WHEN H.INCOME_DESC = '250K+' THEN 250000 ELSE 0 END AS income_range, COUNT(income_desc) AS number_of_rows FROM household.household_spending_partitions H
    GROUP BY INCOME_DESC
)




-- COMMAND ----------



CREATE OR REPLACE TEMP VIEW calcs_income AS(

    SELECT H.income_desc, MIN(total_spending) AS min_ts, MAX(total_spending) AS max_ts, CAST(AVG(total_spending) AS DECIMAL (10,3)) AS avg_ts, CAST(MEDIAN(total_spending) AS DECIMAL (10,3)) AS median_ts, CAST(STD(total_spending) AS DECIMAL (10,3)) AS stddev_ts, MIN(total_coupon_spending) AS min_tcs, MAX(total_coupon_spending) AS max_tcs, CAST(AVG(total_coupon_spending) AS DECIMAL (10,3)) AS avg_tcs, CAST(MEDIAN(total_coupon_spending) AS DECIMAL (10,3)) AS median_tcs, CAST(STD(total_coupon_spending) AS DECIMAL (10,3)) AS stddev_tcs, MIN(effort_rate) AS min_er, MAX(effort_rate) AS max_er, CAST(AVG(effort_rate) AS DECIMAL (10,3)) AS avg_er, CAST(MEDIAN(effort_rate) AS DECIMAL (10,3)) AS median_er, CAST(STD(effort_rate) AS DECIMAL (10,3)) AS stddev_er FROM household.household_spending_partitions H
    GROUP BY H.INCOME_DESC

)

-- COMMAND ----------



CREATE EXTERNAL TABLE IF NOT EXISTS household.query_months_income
USING PARQUET
LOCATION "/user/hive/warehouse/household/query_months_income"
AS
  SELECT H.income_desc, R.number_of_rows, H.min_ts, H.max_ts, H.avg_ts, H.median_ts, H.stddev_ts, H.min_tcs, H.max_tcs, H.avg_tcs, H.median_tcs, H.stddev_tcs, H.min_er, H.max_er, H.avg_er, H.median_er, H.stddev_er FROM calcs_income H
  INNER JOIN number_rows_income R ON R.income_desc = H.income_desc
  ORDER BY R.income_range


-- COMMAND ----------



CREATE OR REPLACE TEMP VIEW number_rows_age AS(

    SELECT age_desc, COUNT(age_desc) AS number_of_rows FROM household.household_spending_partitions H
    GROUP BY age_desc
    ORDER BY age_desc
)

-- COMMAND ----------



CREATE OR REPLACE TEMP VIEW calcs_age AS(

    SELECT H.age_desc, MIN(total_spending) AS min_ts, MAX(total_spending) AS max_ts, CAST(AVG(total_spending) AS DECIMAL (10,3)) AS avg_ts, CAST(MEDIAN(total_spending) AS DECIMAL (10,3)) AS median_ts, CAST(STD(total_spending) AS DECIMAL (10,3)) AS stddev_ts, MIN(total_coupon_spending) AS min_tcs, MAX(total_coupon_spending) AS max_tcs, CAST(AVG(total_coupon_spending) AS DECIMAL (10,3)) AS avg_tcs, CAST(MEDIAN(total_coupon_spending) AS DECIMAL (10,3)) AS median_tcs, CAST(STD(total_coupon_spending) AS DECIMAL (10,3)) AS stddev_tcs, MIN(effort_rate) AS min_er, MAX(effort_rate) AS max_er, CAST(AVG(effort_rate) AS DECIMAL (10,3)) AS avg_er, CAST(MEDIAN(effort_rate) AS DECIMAL (10,3)) AS median_er, CAST(STD(effort_rate) AS DECIMAL (10,3)) AS stddev_er FROM household.household_spending_partitions H
    GROUP BY H.age_desc
    ORDER BY H.age_desc

)

-- COMMAND ----------



CREATE EXTERNAL TABLE IF NOT EXISTS household.query_months_age
USING PARQUET
LOCATION "/user/hive/warehouse/household/query_months_age"
AS
    SELECT H.age_desc, R.number_of_rows, H.min_ts, H.max_ts, H.avg_ts, H.median_ts, H.stddev_ts, H.min_tcs, H.max_tcs, H.avg_tcs, H.median_tcs, H.stddev_tcs, H.min_er, H.max_er, H.avg_er, H.median_er, H.stddev_er FROM calcs_age H
    INNER JOIN number_rows_age R ON R.age_desc = H.age_desc
    ORDER BY H.age_desc

-- COMMAND ----------



CREATE OR REPLACE TEMP VIEW number_rows_month_income AS(

    SELECT income_desc, COUNT(income_desc) AS number_of_rows FROM household.household_spending_partitions H
    WHERE H.year_month = '2022-11'
    GROUP BY H.INCOME_DESC
)

-- COMMAND ----------



CREATE OR REPLACE TEMP VIEW calcs_month_income AS(

    SELECT H.income_desc, H.age_desc, MIN(total_spending) AS min_ts, MAX(total_spending) AS max_ts, CAST(AVG(total_spending) AS DECIMAL (10,3)) AS avg_ts, CAST(MEDIAN(total_spending) AS DECIMAL (10,3)) AS median_ts, CAST(STD(total_spending) AS DECIMAL (10,3)) AS stddev_ts, MIN(total_coupon_spending) AS min_tcs, MAX(total_coupon_spending) AS max_tcs, CAST(AVG(total_coupon_spending) AS DECIMAL (10,3)) AS avg_tcs, CAST(MEDIAN(total_coupon_spending) AS DECIMAL (10,3)) AS median_tcs, CAST(STD(total_coupon_spending) AS DECIMAL (10,3)) AS stddev_tcs, MIN(effort_rate) AS min_er, MAX(effort_rate) AS max_er, CAST(AVG(effort_rate) AS DECIMAL (10,3)) AS avg_er, CAST(MEDIAN(effort_rate) AS DECIMAL (10,3)) AS median_er, CAST(STD(effort_rate) AS DECIMAL (10,3)) AS stddev_er FROM household.household_spending_partitions H
    GROUP BY H.INCOME_DESC, H.age_desc

)

-- COMMAND ----------



  SELECT H.income_desc, R.number_of_rows, H.min_ts, H.max_ts, H.avg_ts, H.median_ts, H.stddev_ts, H.min_tcs, H.max_tcs, H.avg_tcs, H.median_tcs, H.stddev_tcs, H.min_er, H.max_er, H.avg_er, H.median_er, H.stddev_er FROM calcs_month_income H
  INNER JOIN number_rows_month_income R ON R.income_desc = H.income_desc
  ORDER BY H.age_desc

-- COMMAND ----------



CREATE OR REPLACE TEMP VIEW number_rows_month_age AS(

    SELECT age_desc, H.year_month, COUNT(age_desc) AS number_of_rows FROM household.household_spending_partitions H
    WHERE H.year_month = '2022-11'
    GROUP BY H.age_desc, H.year_month
)

-- COMMAND ----------



CREATE OR REPLACE TEMP VIEW calcs_month_age AS(

    SELECT H.age_desc, MIN(total_spending) AS min_ts, MAX(total_spending) AS max_ts, CAST(AVG(total_spending) AS DECIMAL (10,3)) AS avg_ts, CAST(MEDIAN(total_spending) AS DECIMAL (10,3)) AS median_ts, CAST(STD(total_spending) AS DECIMAL (10,3)) AS stddev_ts, MIN(total_coupon_spending) AS min_tcs, MAX(total_coupon_spending) AS max_tcs, CAST(AVG(total_coupon_spending) AS DECIMAL (10,3)) AS avg_tcs, CAST(MEDIAN(total_coupon_spending) AS DECIMAL (10,3)) AS median_tcs, CAST(STD(total_coupon_spending) AS DECIMAL (10,3)) AS stddev_tcs, MIN(effort_rate) AS min_er, MAX(effort_rate) AS max_er, CAST(AVG(effort_rate) AS DECIMAL (10,3)) AS avg_er, CAST(MEDIAN(effort_rate) AS DECIMAL (10,3)) AS median_er, CAST(STD(effort_rate) AS DECIMAL (10,3)) AS stddev_er FROM household.household_spending_partitions H
    GROUP BY H.age_desc

)

-- COMMAND ----------



    SELECT H.age_desc, R.year_month, R.number_of_rows, H.min_ts, H.max_ts, H.avg_ts, H.median_ts, H.stddev_ts, H.min_tcs, H.max_tcs, H.avg_tcs, H.median_tcs, H.stddev_tcs, H.min_er, H.max_er, H.avg_er, H.median_er, H.stddev_er FROM calcs_month_age H
    INNER JOIN number_rows_month_age R ON R.age_desc = H.age_desc
    ORDER BY H.age_desc