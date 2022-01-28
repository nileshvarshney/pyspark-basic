import imp
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


spark = SparkSession.builder.appName("spark-dataframe").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schemaPeople = spark.read.option("header", "true")\
    .option("inferSchema", "true").csv("datasets/fakefriends-header.csv")

print(schemaPeople.printSchema())

print("Let's display the name column")
schemaPeople.select("name").show(5)

print("Let's print people count under 21")
schemaPeople.filter("age < 21").groupBy("age").count().orderBy("age").show()

print("Lets make every one 10 years older")
schemaPeople.select(schemaPeople.name, schemaPeople.age + 10)\
    .show(5, truncate=False)

# average age of the friends
schemaPeople.groupBy("age").avg("friends").sort("age").show()


schemaPeople.groupBy("age").agg(f.round(f.avg("friends"), 2)
                                .alias("avg_friends")).sort("age").show()

spark.stop()
