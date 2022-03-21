from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SimpleApp").master("spark://master:7077").config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1').config("spark.mongodb.input.uri", "mongodb://admin:admin@192.168.2.53:9800/RedditComments.test").config("spark.mongodb.output.uri", "mongodb://admin:admin@192.168.2.53:9800/RedditComments.test").getOrCreate()
df = spark.read.format("mongo").load()

df.printSchema()
spark.stop()


