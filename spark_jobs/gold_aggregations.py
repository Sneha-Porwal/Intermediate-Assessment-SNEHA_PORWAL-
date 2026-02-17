from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, month, year
import os

# -------------------------------------------------
# Spark Session
# -------------------------------------------------
spark = SparkSession.builder \
    .appName("GoldLayer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# -------------------------------------------------
# Paths
# -------------------------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

silver_path = os.path.join(BASE_DIR, "data", "silver")
gold_path = os.path.join(BASE_DIR, "data", "gold")

os.makedirs(gold_path, exist_ok=True)

# -------------------------------------------------
# Load Silver Data
# -------------------------------------------------
transactions = spark.read.csv(
    os.path.join(silver_path, "fact_transactions"),
    header=True,
    inferSchema=True
)

customers = spark.read.csv(
    os.path.join(silver_path, "dim_customers"),
    header=True,
    inferSchema=True
)

#  Monthly Revenue
monthly_revenue = transactions \
    .withColumn("year", year(col("transaction_date"))) \
    .withColumn("month", month(col("transaction_date"))) \
    .groupBy("year", "month") \
    .agg(sum("amount").alias("total_revenue")) \
    .orderBy("year", "month")

monthly_revenue.write.mode("overwrite") \
    .option("header", True) \
    .csv(os.path.join(gold_path, "monthly_revenue"))

print("Monthly Revenue Created")

#  Sales by Region

sales_by_region = transactions.join(
    customers.select("customer_id", "region"),
    on="customer_id",
    how="inner"
).groupBy("region") \
 .agg(sum("amount").alias("total_sales")) \
 .orderBy(col("total_sales").desc())

sales_by_region.write.mode("overwrite") \
    .option("header", True) \
    .csv(os.path.join(gold_path, "sales_by_region"))

print("Sales by Region Created")

#  KPI Summary Table
kpi_summary = transactions.agg(
    sum("amount").alias("total_revenue"),
    count("transaction_id").alias("total_transactions")
)

kpi_summary.write.mode("overwrite") \
    .option("header", True) \
    .csv(os.path.join(gold_path, "kpi_summary"))

print("KPI Summary Created")

print("\nGold Layer Created Successfully!")

spark.stop()
