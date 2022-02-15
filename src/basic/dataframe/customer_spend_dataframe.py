"""
    Steps:
    1. Load customer-orders.csv
    2. group by cust_id
    3. sum the amount spent (round to 2 decimal place)
    4. show the result
"""
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName('customer spent').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("customerID", IntegerType(), True),
    StructField("ReceiptID",  IntegerType(), True),
    StructField("Spent", FloatType(), True)
])

df = spark.read.schema(schema=schema).csv(
    "datasets/customer-orders.csv")
df.printSchema()

customerSpent = df.groupBy("customerID").agg(
    f.round(f.sum("Spent"), 2).alias("total_spent")).sort("total_spent", ascending=False)

customerSpent.show(5, truncate=False)

spark.stop()
