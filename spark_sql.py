from pyspark.sql import SparkSession
from pyspark.sql import Row


def mapper(line):
    fields = line.split(",")
    return Row(
        ID=int(fields[0]),
        name=str(fields[1].encode('utf-8')),
        age=int(fields[2]),
        numFriends=int(fields[3])
    )


# get spark sesssion
spark = SparkSession.builder.appName("Teenager Friends count").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
lines = spark.sparkContext.textFile("datasets/fakefriends.csv")
people = lines.map(mapper)

# infer the schema and register the dataframe
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

teenagers = spark.sql("select * from people where age >= 13 and age <= 19")
for teen in teenagers.collect():
    print(teen)

schemaPeople.groupBy("age").count().orderBy("age", ascending=False).show()

spark.stop()
