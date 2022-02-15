import re
from pyspark import SparkConf, SparkContext


def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

# initialise spark context
conf = SparkConf().setMaster("local").setAppName("Words Count")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# input dataset
input = sc.textFile("datasets/Book.txt")

# transformation
words = input.flatMap(normalizeWords)
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
flipped = wordCounts.map(lambda x :(x[1], x[0])).sortByKey(ascending=True)
flipped_again = flipped.map(lambda x :(x[1], x[0]))
results = flipped_again.collect()

# wordCounts = words.countByValue()

for result in results:
    cleanWord = result[0].encode('ascii', 'ignore')
    # only include words
    if cleanWord.isalpha():
        print(cleanWord.decode("utf-8"), ' => ', result[1])
