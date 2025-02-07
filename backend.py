from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, mean
from datetime import date

spark = SparkSession.builder.master("spark://192.168.1.13:7077") \
    .appName("QueryBackend") \
    .getOrCreate()

df = spark.read.json("output")

result = df

def mean_polarity(label):
    global result

    result = result.filter(result.aidr_label == label)
    return result.agg(mean(result.sentiment_polarity)).collect()[0][0]  

def query(labels = None, verified = None, created_at = None, created_at_end = None, possibly_sensitive = None, place = None, coordinates = None, groupByValue = None):
    global df
    global result 
    df = df.filter(label_parser(labels) & verified_parser(verified) & possibly_sensitive_parser(possibly_sensitive) & date_parser(created_at, created_at_end) & place_parser(place))
    result = df
    return df.toPandas()

def label_parser(labels):
    return (col("aidr_label").isin(labels)) if len(labels) > 0 else lit(True)

def date_parser(created_at, created_at_end):

    if created_at_end is None:
        return (col("date") == created_at) if created_at is not None else lit(True)

def place_parser(place):
    return( (col("place_state").isin(place) if len(place) > 0 else lit(True)))

def verified_parser(verified):
    return (col("verified") == True) if verified == "True" else (col("verified") == False) if verified == "False" else lit(True)

def possibly_sensitive_parser(ps):
     return (col("possibly_sensitive") == True) if ps == 1 else (col("possibly_sensitive") == False) if ps == 0 else lit(True)