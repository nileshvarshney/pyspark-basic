from curses.ascii import SP
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StructType, StructField,  StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("minimum tempreture").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

schema = StructType([
    StructField("stationID", StringType(), True),
    StructField("date", IntegerType(), True),
    StructField("measure_type", StringType(), True),
    StructField("tempreture", FloatType(), True)
])

df = spark.read.schema(schema=schema).csv("datasets/1800.csv")
df.printSchema()

minTempreture = df.filter(df.measure_type == "TMIN")

minStationTempretures = minTempreture.groupBy(
    "stationID").min("tempreture")

minStationTempreturesF = minStationTempretures.withColumn(
    "tempreture", f.round(
        f.col("min(tempreture)") * (9.0/5.0) + 32, 2
    )).select("stationID", "tempreture").sort("tempreture").collect()

for result in minStationTempreturesF:
    print(result[0], " ", result[1])

spark.stop()
