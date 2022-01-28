from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import regexp_extract

# 66.249.75.159 - - [29/Nov/2015:03:50:05 +0000] "GET /robots.txt HTTP/1.1" 200 55 "-" "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
# 66.249.75.168 - - [29/Nov/2015:03:50:06 +0000] "GET /blog/ HTTP/1.1" 200 8083 "-" "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"

spark = SparkSession.builder.appName("StructureStreaming").getOrCreate()

accessLog = spark.read.text("datasets/log/access_log.txt")

contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logDF = accessLog.select(regexp_extract('value', hostExp, 1).alias("host"),
                         regexp_extract('value', timeExp,
                                        1).alias("timestamp"),
                         regexp_extract('value', generalExp,
                                        1).alias("method"),
                         regexp_extract('value', generalExp,
                                        2).alias("endpoint"),
                         regexp_extract('value', generalExp,
                                        3).alias("protocol"),
                         regexp_extract('value', statusExp,
                                        1).cast("integer").alias("status"),
                         regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))


logDF.show(20)
