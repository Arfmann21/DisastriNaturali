from pyspark.sql import SparkSession

spark = SparkSession.builder.master("spark://192.168.1.13:7077") \
    .appName("QueryBackend") \
    .getOrCreate()

df = spark.read.json("output").createOrReplaceTempView("Test")

def query():
    return spark.sql("SELECT name FROM Test WHERE verified = true").collect()
