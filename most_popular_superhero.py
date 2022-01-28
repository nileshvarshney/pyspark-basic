from importlib_metadata import collections
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructField, StructType, StringType, IntegerType


spark = SparkSession.builder.appName('Superhero Name').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

schema = StructType([
    StructField('Id', IntegerType(), True),
    StructField("name", StringType(), True)
])

names = spark.read.option("sep", " ").schema(
    schema=schema).csv("datasets/MarvelNames.txt")

lines = spark.read.text("datasets/MarvelGraph.txt")
connections = lines.withColumn("id", f.split(f.col("value"), " ")[0]).withColumn("connections", f.size(
    f.split(f.col("value"), " ")) - 1).groupBy("id").agg(f.sum("connections").alias("collections")).sort(f.col("collections").desc())

collections_ascending = connections.sort(f.col("collections"))
mostPopular = connections.first()
mostObscure = collections_ascending.first()

mostPopularName = names.filter(
    f.col("id") == mostPopular[0]).select("name").first()
mostObscureName = names.filter(
    f.col("id") == mostObscure[0]).select("name").first()

print(mostPopularName[0] + ' is most popular superhere with ' +
      str(mostPopular[1]) + ' collections')

print(mostObscureName[0] + ' is most Obscure superhere with ' +
      str(mostObscure[1]) + ' collections')
spark.stop()
