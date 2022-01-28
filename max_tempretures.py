from pyspark import SparkConf, SparkContext


def parseLine(line):
    fields = line.split(',')
    stationId = fields[0]
    entryType = fields[2]
    tempreture = fields[3]
    return (stationId, entryType, tempreture)


conf = SparkConf().setMaster("local").setAppName("Maximum Tempreture")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

lines = sc.textFile("datasets/1800.csv")
rdd = lines.map(parseLine)
maxTempreture = rdd.filter(lambda x: "TMAX" in x[1])
stationTemp = maxTempreture.map(lambda x: (x[0], x[2]))
maxTemps = stationTemp.reduceByKey(lambda x, y: max(x, y))
results = maxTemps.collect()
for result in results:
    print(result)
