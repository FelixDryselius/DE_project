# To run this script use:
# spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 <PATH_TO_THIS_SCRIPT>

import time
from pyspark.sql import SparkSession
start_time = time.time()

spark = SparkSession.builder.appName("jan_small").master("spark://master:7077")\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1')\
    .config("spark.executor.memory", "2g")\
    .config("spark.mongodb.input.database", "RedditComments")\
    .config("spark.mongodb.input.collection", "jan_2010")\
    .config("spark.mongodb.input.uri", "mongodb://192.168.2.14:27018, 192.168.2.129:27018/?readPreference=nearest")\
    .config("spark.mongodb.input.localThreshold",5)\
    .config("spark.mongodb.input.partitioner" ,"MongoShardedPartitioner") \
    .config("spark.mongodb.input.partitionerOptions.shardkey", "created_utc") \
    .getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

df = spark.read.format("mongo").option("query", "SELECT * FROM body").load()
#df.show()

def getWordCount(df, word):
	dfFiltered = df.filter(df.body.rlike("(?i:"+word+")"))
	return dfFiltered.count()

def printWordsAndCounts(df, words):
    for word in words:
        print(word+"="+str(getWordCount(df, word)))

words=['Russia', 'Russo', 'Ukraine', 'Ukrainian']


printWordsAndCounts(df,words)

spark.stop()
stop_time = time.time()
time_execution = stop_time - start_time

print("Time execution:" + str(time_execution )+ " sec")
