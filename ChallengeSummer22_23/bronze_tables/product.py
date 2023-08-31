# Databricks notebook source
# File location and type
file_location = "/FileStore/tables/product_v2.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "product_temp"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE products;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS product AS
# MAGIC     SELECT * FROM product_temp;