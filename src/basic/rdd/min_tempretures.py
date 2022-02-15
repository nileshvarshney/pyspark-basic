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

# minimum tempretue
minTempreture = rdd.filter(lambda x: "TMIN" in x[1])
stationMinTemp = minTempreture.map(lambda x: (x[0], x[2]))

# maximum tempreture
maxTempreture = rdd.filter(lambda x: "TMAX" in x[1])
stationMaxTempreture = maxTempreture.map(lambda x:(x[0], x[2]))

minTemps = stationMinTemp.reduceByKey(lambda x, y: min(x, y))
maxTemps = stationMaxTempreture.reduceByKey(lambda x, y: max(x, y))

res_min_temp = minTemps.collect()
res_max_temp = maxTemps.collect()
print("{} : \t{} \t{}".format("StationID","Min","Max"))
for result in zip(res_min_temp,res_max_temp):
    print("{} : \t{} \t{}".format(result[0][0], result[0][1], result[1][1]))
