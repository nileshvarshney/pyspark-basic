from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, LongType
import sys


def computeCosineSimilarity(spark, data):
    moviePairs = data.alias("ratings1")\
        .join(data.alias("ratings2"), (f.col("ratings1.userID") == f.col("ratings2.userID")) &
              (f.col("ratings1.movieID") < f.col("ratings2.movieID")))\
        .select(
        f.col("ratings1.movieID").alias("movie1"),
        f.col("ratings2.movieID").alias("movie2"),
        f.col("ratings1.rating").alias("rating1"),
        f.col("ratings2.rating").alias("rating2"))

    pairScores = moviePairs.withColumn("xx", f.col("rating1") * f.col("rating1"))\
        .withColumn("yy", f.col("rating2") * f.col("rating2"))\
        .withColumn("xy", f.col("rating1") * f.col("rating2"))

    calculatedSimilarity = pairScores.groupBy("movie1", "movie2").agg(
        f.sum(f.col("xy")).alias("numerator"),
        (f.sqrt(f.sum(f.col("xx"))) + f.sqrt(f.sum(f.col("yy")))).alias("denominator"),
        f.count(f.col("rating1")).alias("numPairs"))

    results = calculatedSimilarity.withColumn("score", f.when(f.col("numerator") != 0, f.col(
        "numerator")/f.col("denominator")).otherwise(0))\
        .select("movie1", "movie2", "score", "numPairs")

    return results


def getMovieName(movieID):
    result = movieNames.filter(f.col("movieID") == movieID).select(
        "MovieTitle").collect()[0]
    return result[0]


spark = SparkSession.builder.appName(
    "Movie Similarity").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

movieNameSchema = StructType([
    StructField("MovieID", IntegerType(), True),
    StructField("MovieTitle", StringType(), True)
])

movieSchema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

movieNames = spark.read.option("sep", "|").option(
    "charset", "ISO-8859-1").schema(movieNameSchema).csv('datasets/u.item')

movies = spark.read.option("sep", "\t").schema(
    movieSchema).csv("datasets/u.data")

ratings = movies.select("userID", "movieID", "rating")

moviePairsSimilarity = computeCosineSimilarity(spark, ratings).cache()

if (len(sys.argv) > 1):
    scoreThreshold = 0.97
    coOccuranceThreshold = 50.0

    movieID = int(sys.argv[1])

    filteredResults = moviePairsSimilarity.filter(
        ((f.col("movie1") == movieID) | (f.col("movie2") == movieID)) &
        (f.col("score") > scoreThreshold) & (f.col("numPairs") > coOccuranceThreshold))

    results = filteredResults.sort(f.col("score").desc()).take(10)
    print("Top 10 Similar movies for " + getMovieName(movieID))
    for result in results:
        similarMovieId = result.movie1
        if similarMovieId == movieID:
            similarMovieId = result.movie2
        print('{:60s} score {:10.2f} strength {}'.format(
            getMovieName(similarMovieId), result.score, result.numPairs))
