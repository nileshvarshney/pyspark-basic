from pyspark import SparkConf, SparkContext


def parseIt(line):
    fields = line.split(",")
    customer = fields[0]
    spent = float(fields[2])
    return (customer, spent)


conf = SparkConf().setMaster("local").setAppName("Customer Spend")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")


input = sc.textFile("datasets/customer-orders.csv")
rdd = input.map(parseIt)
totalByCustomer = rdd.reduceByKey(lambda x, y: x + y)
flipped = totalByCustomer.map(lambda x: (x[1], x[0]))
sortedBySpent = flipped.sortByKey(False)
for result in sortedBySpent.collect():
    print(result)
