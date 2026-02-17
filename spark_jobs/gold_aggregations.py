from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, month, year
import os

# -------------------------------------------------
# Spark Session
# -------------------------------------------------
spark = SparkSession.builder.appName("GoldLayer").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

os.makedirs("data/gold", exist_ok=True)

# Load Silver Data
transactions = spark.read.csv(
    "data/silver/fact_transactions",
    header=True,
    inferSchema=True
)

customers = spark.read.csv(
    "data/silver/dim_customers",
    header=True,
    inferSchema=True
)

# 1 Monthly Revenue
monthly_revenue = transactions \
    .withColumn("year", year(col("transaction_date"))) \
    .withColumn("month", month(col("transaction_date"))) \
    .groupBy("year", "month") \
    .agg(sum("amount").alias("total_revenue")) \
    .orderBy("year", "month")

monthly_revenue.write.mode("overwrite") \
    .option("header", True) \
    .csv("data/gold/monthly_revenue")

print("Monthly Revenue Created")

#  Sales by Region
sales_by_region = transactions.join(
    customers.select("customer_id", "region"),
    "customer_id",
    "inner"
).groupBy("region") \
 .agg(sum("amount").alias("total_sales")) \
 .orderBy(col("total_sales").desc())

sales_by_region.write.mode("overwrite") \
    .option("header", True) \
    .csv("data/gold/sales_by_region")

print("Sales by Region Created")

# KPI Summary
kpi_summary = transactions.agg(
    sum("amount").alias("total_revenue"),
    count("transaction_id").alias("total_transactions")
)

kpi_summary.write.mode("overwrite") \
    .option("header", True) \
    .csv("data/gold/kpi_summary")

print("KPI Summary Created")

print("\nGold Layer Created Successfully!")

spark.stop()
