# Databricks notebook source
from pyspark.sql.types import IntegerType, DecimalType, StructType, StructField, LongType

transaction_data_schema = StructType([
    StructField("household_key", IntegerType(), True),
    StructField("BASKET_ID", LongType(), True),
    StructField("DAY", IntegerType(), True),
    StructField("PRODUCT_ID", IntegerType(), True),
    StructField("QUANTITY", IntegerType(), True),
    StructField("SALES_VALUE", DecimalType(10,3), True),
    StructField("STORE_ID", IntegerType(), True),
    StructField("COUPON_MATCH_DISC", DecimalType(10,3), True),
    StructField("COUPON_DISC", DecimalType(10,3), True),
    StructField("RETAIL_DISC", DecimalType(10,3), True),
    StructField("TRANS_TIME", IntegerType(), True),
    StructField("WEEK_NO", IntegerType(), True)
])

# File location and type
file_location = "/FileStore/tables/transaction_data_v2.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .schema(transaction_data_schema).load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "transaction_data_temp"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE household;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS transaction_data AS
# MAGIC     SELECT * FROM transaction_data_temp;