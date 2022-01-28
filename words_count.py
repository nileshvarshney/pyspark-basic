import re
from pyspark import SparkConf, SparkContext


def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("Words Count")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")


input = sc.textFile("datasets/Book.txt")
words = input.flatMap(normalizeWords)
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
# wordCountSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCounts.collect()
# wordCounts = words.countByValue()

for result in results:
    cleanWord = result[0].encode('ascii', 'ignore')

    if cleanWord.isalpha():
        print(cleanWord.decode("utf-8"), ' => ', result[1])
