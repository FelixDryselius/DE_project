# To run this script use:
# spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 <PATH_TO_THIS_SCRIPT>

import time
from pyspark.sql import SparkSession
start_time = time.time()

spark = SparkSession.builder.appName("jan_small").master("spark://master:7077")\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1')\
    .config("spark.executor.memory", "2560m")\
    .config("spark.mongodb.input.database", "RedditComments")\
    .config("spark.mongodb.input.collection", "jan_2010")\
    .config("spark.mongodb.input.uri", "mongodb://192.168.2.14:27018, 192.168.2.129:27018/?readPreference=nearest")\
    .config("spark.mongodb.input.localThreshold",10)\
    .config("spark.mongodb.input.partitioner" ,"MongoShardedPartitioner") \
    .config("spark.mongodb.input.partitionerOptions.shardkey", "created_utc") \
    .getOrCreate()

sc = spark.sparkContext

sc.setLogLevel("ERROR")

#.option("query", "SELECT * FROM body")
df = spark.read.format("mongo").option("query", "SELECT * FROM body").load()
#df.show()

def getWordCount(df, word):
	dfFiltered = df.filter(df.body.rlike("(?i:"+word+")"))
	#dfSplitted = dfFiltered.filter(lambda body: isBodyContainsWord(body,word))
	return dfFiltered.count()

def printWordsAndCounts(df, words):
    for word in words:
        print(word+"="+str(getWordCount(df, word)))

words=['Russia', 'Russo', 'Ukraine', 'Ukrainian', 'Donbass']


def filterDataFrameByWords(df, words):
    found = False
    for word in words:
        if df.body.rlike("(?i:"+word+")"):
            return True
    return found

printWordsAndCounts(df,words)

def getConcanatedWords(words):
    concan = ''
    for word in words:
        if(concan == ''):
            concan += word
        else:
            concan += '|' + word
    return concan

#dfFiltered = df.filter(df.body.rlike("(?i:"+getConcanatedWords(words)+")"))
#dfFiltered.show(truncate=False)

#words2 = ['war', 'propaganda', 'manipulation', 'occupy', 'occupation', 'crisis', 'energy security', 'invasion', 'conflict', 'invade', 'invading', 'invasion', 'hostile', 'aggression', 'crimea', 'covert-ops', 'information war']
#printWordsAndCounts(dfFiltered,words2)

spark.stop()
stop_time = time.time()
time_execution = stop_time - start_time

print("Time execution:" + str(time_execution )+ " sec")


def isBodyContainsWord(body, word):
	found = False
	splits = body.split()
	for split in splits:
		if split.lower() == word.lower():
			return True
	return found


