# Databricks notebook source
transaction_data_enriched = spark.read.table("household.transaction_data_enriched")
coupon_enriched = spark.read.table("coupons.coupon_enriched")

total_coupon_spending = transaction_data_enriched.join(coupon_enriched, (transaction_data_enriched.household_key == coupon_enriched.household_key) & (transaction_data_enriched.trans_date == coupon_enriched.trans_date) & (transaction_data_enriched.PRODUCT_ID == coupon_enriched.product_id), "inner") \
    .where(coupon_enriched.is_coupon_redeemed == 'true') \
    .select(transaction_data_enriched.household_key, transaction_data_enriched.trans_date, transaction_data_enriched.SALES_VALUE)

total_coupon_spending = total_coupon_spending.selectExpr("household_key", "date_format(trans_date, 'yyyy-MM') as date", "SALES_VALUE").groupBy("household_key","date") \
    .sum("SALES_VALUE") \
    .withColumnRenamed("sum(SALES_VALUE)","SALES_VALUE")

# COMMAND ----------

total_spending = transaction_data_enriched.selectExpr("household_key","date_format(trans_date, 'yyyy-MM') as date", "SALES_VALUE") \
    .groupBy("household_key","date").sum("SALES_VALUE") \
    .withColumnRenamed("sum(SALES_VALUE)","SALES_VALUE")

# COMMAND ----------

from pyspark.sql.functions import when
from pyspark.sql.types import DecimalType

hh_demographic_enriched = spark.read.table("household.hh_demographic_enriched")

effort_rate = total_spending.join(hh_demographic_enriched, (hh_demographic_enriched.household_key == total_spending.household_key), "inner") \
    .select(total_spending.household_key, total_spending.date, total_spending.SALES_VALUE, hh_demographic_enriched.INCOME_DESC)

effort_rate = effort_rate.select(effort_rate.household_key, effort_rate.date, effort_rate.SALES_VALUE, when(effort_rate.INCOME_DESC == 'Under 15K', effort_rate.SALES_VALUE/(8400/12)).when(effort_rate.INCOME_DESC == '15-24K',effort_rate.SALES_VALUE/(15000/12)).when(effort_rate.INCOME_DESC == '25-34K', effort_rate.SALES_VALUE/(25000/12)).when(effort_rate.INCOME_DESC == '35-49K', effort_rate.SALES_VALUE/(35000/12)).when(effort_rate.INCOME_DESC == '50-74K', effort_rate.SALES_VALUE/(50000/12)).when(effort_rate.INCOME_DESC == '75-99K', effort_rate.SALES_VALUE/(75000/12)).when(effort_rate.INCOME_DESC == '100-124K', effort_rate.SALES_VALUE/(100000/12)).when(effort_rate.INCOME_DESC == '125-149K', effort_rate.SALES_VALUE/(125000/12)).when(effort_rate.INCOME_DESC == '150-174K', effort_rate.SALES_VALUE/(150000/12)).when(effort_rate.INCOME_DESC == '175-199K', effort_rate.SALES_VALUE/(175000/12)).when(effort_rate.INCOME_DESC == '200-249K',effort_rate.SALES_VALUE/(200000/12)).when(effort_rate.INCOME_DESC == '250K+',effort_rate.SALES_VALUE/(250000/12)).otherwise(0).cast(DecimalType(20,3)).alias('effort_rate'))

# COMMAND ----------

household_spending_partitions_py = hh_demographic_enriched.join(total_spending, hh_demographic_enriched.household_key == total_spending.household_key, "inner").join(total_coupon_spending, (hh_demographic_enriched.household_key == total_coupon_spending.household_key) & (total_coupon_spending.date == total_spending.date), "inner").select(hh_demographic_enriched.household_key,  hh_demographic_enriched.AGE_DESC,  hh_demographic_enriched.MARITAL_STATUS_CODE,  hh_demographic_enriched.INCOME_DESC,  hh_demographic_enriched.HOMEOWNER_DESC,  hh_demographic_enriched.HH_COMP_DESC,  hh_demographic_enriched.HOUSEHOLD_SIZE_DESC,  hh_demographic_enriched.KID_CATEGORY_DESC, total_spending.date, total_spending.SALES_VALUE.alias("total_spending"), total_coupon_spending.SALES_VALUE.alias("total_coupon_spending"))

household_spending_partitions_py = household_spending_partitions_py.join(effort_rate, (household_spending_partitions_py.household_key == effort_rate.household_key) & (effort_rate.date == household_spending_partitions_py.date), "inner").select(household_spending_partitions_py.household_key,  household_spending_partitions_py.AGE_DESC,  household_spending_partitions_py.MARITAL_STATUS_CODE,  household_spending_partitions_py.INCOME_DESC,  household_spending_partitions_py.HOMEOWNER_DESC,  household_spending_partitions_py.HH_COMP_DESC, household_spending_partitions_py.HOUSEHOLD_SIZE_DESC, household_spending_partitions_py.KID_CATEGORY_DESC, household_spending_partitions_py.date.alias("year_month"), household_spending_partitions_py.total_spending, household_spending_partitions_py.total_coupon_spending, effort_rate.effort_rate)


# COMMAND ----------

household_spending_partitions_py.repartition(4).write.format("parquet").mode("overwrite").option("path","/user/hive/warehouse/household/household_spending_partitions_py").saveAsTable("household.household_spending_partitions_py")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM household.household_spending_partitions_py