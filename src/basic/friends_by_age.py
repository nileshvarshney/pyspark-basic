from pyspark import SparkConf, SparkContext

# parse CSV line
def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)


conf = SparkConf().setMaster("local").setAppName("Friends by Age")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

lines = sc.textFile("datasets/fakefriends.csv")
rdd = lines.map(parseLine)

# transformation
totalByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda a,b : (a[0]+ a[1], b[0] +b[1]))

averageByAge = totalByAge.mapValues(lambda x: x[0] / x[1])
results = averageByAge.collect()

for result in results:
    print(result)
