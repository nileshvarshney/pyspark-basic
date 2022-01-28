from pyspark import SparkConf, SparkContext


def parseLine(line):
    fields = line.split(',')
    stationId = fields[0]
    entryType = fields[2]
    tempreture = fields[3]
    return (stationId, entryType, tempreture)


conf = SparkConf().setMaster("local").setAppName("Minimum Tempreture")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

lines = sc.textFile("datasets/1800.csv")
rdd = lines.map(parseLine)
minTempreture = rdd.filter(lambda x: "TMIN" in x[1])
stationTemp = minTempreture.map(lambda x: (x[0], x[2]))
minTemps = stationTemp.reduceByKey(lambda x, y: min(x, y))
results = minTemps.collect()
for result in results:
    print(result)
