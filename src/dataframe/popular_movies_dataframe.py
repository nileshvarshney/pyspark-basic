from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StructField, StructType, IntegerType, LongType
import codecs


def loadMovieNames():
    movieNames = {}
    with codecs.open("datasets/u.item", 'r', encoding="ISO-8859-1", errors='ignore') as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]
    return movieNames


spark = SparkSession.builder.appName('popular movie').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# broadcast movie names
nameDict = spark.sparkContext.broadcast(loadMovieNames())

schema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("MovieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

moviesDF = spark.read.option("sep", "\t").schema(
    schema=schema).csv("datasets/u.data")

moviesDF.printSchema()

topMoviesID = moviesDF.groupBy("MovieID").count().orderBy(f.desc("count"))


def lookupNames(movieID):
    return nameDict.value[movieID]


lookupNameDF = f.udf(lookupNames)

topMoviesWithTitle = topMoviesID.withColumn(
    "title", lookupNameDF(f.col("MovieID")))


topMoviesWithTitle.show(5, truncate=False)
spark.stop()
