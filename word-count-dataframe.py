from re import I
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName('Word count').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

inputDF = spark.read.text("datasets/Book.txt")

words = inputDF.select(f.explode(f.split(inputDF.value, "\W+")).alias("word"))
words.filter(words.word != "")

lowerCaseWords = words.select(f.lower(words.word).alias("word"))

wordCounts = lowerCaseWords.groupBy("word").count()

wordCountSorted = wordCounts.sort("count", ascending=False)

wordCountSorted.show()
spark.stop()
