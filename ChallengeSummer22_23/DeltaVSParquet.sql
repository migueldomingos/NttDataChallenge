-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta tables VS Parquet tables
-- MAGIC
-- MAGIC In this notebook we will observe some of the differences between the tables mentioned above. 
-- MAGIC
-- MAGIC Firstly, we will present some quick notes to introduce this topic.
-- MAGIC
-- MAGIC Delta
-- MAGIC - Keep versions of data. So we can get any point in time data. That means we can access an older version of data available in the table.
-- MAGIC - Keep transaction logs to track all the commits on the table or delta path. Hence, it creates an additional folder at the data path with the name _delta_log.
-- MAGIC - It allows Insert, Update, and Delete on the data.
-- MAGIC
-- MAGIC Parquet
-- MAGIC - Don’t keep any data version. It keeps any current version of data. We can not retrieve older versions of data.
-- MAGIC - Don’t keep any transaction logs. It holds only data files and status file success.
-- MAGIC - It is mostly used to keep appended data – Insert only.
-- MAGIC
-- MAGIC You will want to use a Delta table when:
-- MAGIC - Many Insert, Delete transactions happened on data
-- MAGIC - Update required for your data
-- MAGIC - Want to keep versions of data
-- MAGIC
-- MAGIC You will want to use a Parquet table when:
-- MAGIC - There is only new data being appended
-- MAGIC - Updates are not required

-- COMMAND ----------

-- DBTITLE 1,Run this command to initialize the schemas (databases)
-- MAGIC %run ./Creating_Databases

-- COMMAND ----------

-- DBTITLE 1,Run this command to initialize the Delta table (campaign_desc)
-- MAGIC %run ./tables/campaign_desc

-- COMMAND ----------

-- DBTITLE 1,Create a copy of the Delta table, we will use this copy in the explanation:
CREATE TABLE IF NOT EXISTS testing.delta_aux_table AS
  SELECT * FROM campaigns.campaign_desc

-- COMMAND ----------

-- DBTITLE 1,If you have not initialised the Parquet table run this command, init the bronze tables and then run this:
-- MAGIC %run ./silver_tables/campaign_enriched

-- COMMAND ----------

-- DBTITLE 1,If you have initialised, but it is not loaded, run this:
CREATE TABLE IF NOT EXISTS campaigns.campaign_enriched (household_key INTEGER, CAMPAIGN INTEGER, DESCRIPTION STRING, START_DAY INTEGER, END_DAY INTEGER, START_DATE STRING, END_DATE STRING, CAMPAIGN_DURATION INTEGER)
USING PARQUET
LOCATION "/user/hive/warehouse/campaigns/campaign_enriched"

-- COMMAND ----------

-- DBTITLE 1,We will use for this explanation an auxiliar Parquet table
CREATE TABLE IF NOT EXISTS testing.aux_table_exercise 
USING PARQUET 
AS
  SELECT * FROM campaigns.campaign_enriched

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Table Versioning
-- MAGIC
-- MAGIC One of the most important aspects of the Delta table is the versioning.
-- MAGIC
-- MAGIC Imagine that you need to delete all the rows where the DESCRIPTION is "TypeB", but, by mistake, you type "TypeC": 

-- COMMAND ----------

DELETE FROM testing.delta_aux_table WHERE DESCRIPTION = "TypeC";

SELECT * FROM testing.delta_aux_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Now you want to go back to the previous version. First, lets check which version we want to rollback:

-- COMMAND ----------

DESCRIBE HISTORY testing.delta_aux_table

-- COMMAND ----------

-- DBTITLE 1,To check the contents of the table based on the version, run this:
SELECT * 
FROM testing.delta_aux_table VERSION AS OF 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC After seeing the versions, we conclude that we want to go back to version 0.
-- MAGIC
-- MAGIC We simply run this:

-- COMMAND ----------

RESTORE TABLE testing.delta_aux_table TO VERSION AS OF 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC With a SELECT you can check if the rollback is successful. However, it is important to know that the rollback is saved in the table´s history:

-- COMMAND ----------

DESCRIBE HISTORY testing.delta_aux_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC In the Parquet table, this versioning is not possible. However, the Parquet table occupies less space.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Small file problem
-- MAGIC
-- MAGIC The second aspect we will be approaching is how we can resolve the small file problem in Delta tables and Parquet tables.
-- MAGIC
-- MAGIC The small file problem slows down the processing of data, because reading through small files involve lots of seeks and lots of hopping between data node to data node, which is inturn inefficient data processing

-- COMMAND ----------

-- DBTITLE 1,Here we will partition both tables to "create" the problem
-- MAGIC %py
-- MAGIC
-- MAGIC df_auxTable = spark.table("testing.aux_table_exercise")
-- MAGIC
-- MAGIC df_auxTable.repartition(100).write.partitionBy("START_DAY").format("delta").saveAsTable("testing.aux_table_exercise_delta")
-- MAGIC df_auxTable.repartition(100).write.partitionBy("START_DAY").format("parquet").saveAsTable("testing.aux_table_exercise_parquet")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Now we have the Parquet table "aux_table_exercise_parquet" and the Delta table "aux_table_exercise_delta".
-- MAGIC
-- MAGIC We can see that the new table is partioned by START_DAY

-- COMMAND ----------

-- DBTITLE 1,If you want to see the partitions from the parquet table, change "aux_table_exercise_delta" to "aux_table_exercise_parquet"
-- MAGIC %fs
-- MAGIC
-- MAGIC ls /user/hive/warehouse/testing.db/aux_table_exercise_delta/START_DAY=224

-- COMMAND ----------

-- DBTITLE 1,In Delta tables, we can use OPTIMIZE to solve this problem
OPTIMIZE testing.aux_table_exercise_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC If you see the number of files on the Delta table, it appears that that number hardly changed. However, note that, in Delta tables, you can roll back to a past version. Which means that you need to have the files stored.
-- MAGIC
-- MAGIC If you want to delete the past files, use the VACCUM command.
-- MAGIC
-- MAGIC In Parquet table the process is more difficult. You need to create a temporary table with the info of the problematic table, set the size of each partition and then overwrite the problematic table with the temp table.

-- COMMAND ----------

CREATE OR REPLACE TABLE tmp_table AS
  SELECT * FROM testing.aux_table_exercise_parquet;

-- COMMAND ----------

-- DBTITLE 0,On the Parquet tables, you need to do this to solve the problem:
SET dfs.blocksize=268435456;
SET parquet.block.size=268435456;

INSERT OVERWRITE TABLE testing.aux_table_exercise_parquet PARTITION (START_DAY)
SELECT * FROM tmp_table;

DROP TABLE IF EXISTS tmp_table;

-- COMMAND ----------

-- DBTITLE 1,Drop the created tables
DROP TABLE IF EXISTS campaigns.campaign_desc;
DROP TABLE IF EXISTS testing.delta_aux_table;
DROP TABLE IF EXISTS campaigns.campaign_enriched;
DROP TABLE IF EXISTS testing.aux_table_exercise;
DROP TABLE IF EXISTS testing.aux_table_exercise_delta;
DROP TABLE IF EXISTS testing.aux_table_exercise_parquet;