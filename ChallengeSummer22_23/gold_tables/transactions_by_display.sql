-- Databricks notebook source
-- DBTITLE 1,We can see that the number of sales "Not on display" is much higher than the rest of the options
--SELECT CDE.display_description, count(TDE.sales_value_euros) AS num_sales
--FROM products.causal_data_enriched CDE INNER JOIN household.transaction_data_enriched TDE ON CDE.product_id = TDE.PRODUCT_ID
--GROUP BY CDE.display_description
--ORDER BY num_sales DESC

-- COMMAND ----------

CREATE OR REPLACE VIEW num_sales_by_display AS
  SELECT CDE.product_id, CDE.display_description, count(TDE.sales_value_euros) AS num_sales
  FROM products.causal_data_enriched CDE INNER JOIN household.transaction_data_enriched TDE ON CDE.product_id = TDE.PRODUCT_ID
  GROUP BY CDE.product_id, CDE.display_description
  ORDER BY CDE.product_id ASC

-- COMMAND ----------

-- DBTITLE 1,Top 3 displays of sale by product
CREATE EXTERNAL TABLE IF NOT EXISTS products.top3_sales_by_display
USING PARQUET
LOCATION "/user/hive/warehouse/products/top3_sales_by_display"
AS
  WITH ranked_sales AS (
    SELECT product_id, display_description, num_sales,
      ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY num_sales DESC) AS row_num
    FROM num_sales_by_display
  )
  SELECT product_id, display_description, num_sales
  FROM ranked_sales
  WHERE row_num = 1 OR row_num = 2 OR row_num = 3
  ORDER BY product_id

-- COMMAND ----------

--WITH ranked_sales AS (
 --   SELECT product_id, display_description, num_sales,
  --    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY num_sales DESC) AS row_num
  --  FROM num_sales_by_display
 -- )
 -- SELECT RS.product_id, RS.display_description, RS.num_sales, date_format(TDE.trans_date, 'yyyy-MM') AS date
--  FROM ranked_sales RS INNER JOIN household.transaction_data_enriched TDE ON RS.product_id = TDE.PRODUCT_ID
--  ORDER BY RS.product_id

-- COMMAND ----------

