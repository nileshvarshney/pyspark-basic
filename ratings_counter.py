from pyspark import SparkContext, SparkConf
import collections

# Set up spark context
conf = SparkConf().setMaster("local").setAppName("Ratings Histogram")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# Read data file
lines = sc.textFile("datasets/u.data")

ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()
# print("************", result)
sorted_results = collections.OrderedDict(sorted(result.items()))
for key, value in sorted_results.items():
    print("%s %i" % (key, value))
