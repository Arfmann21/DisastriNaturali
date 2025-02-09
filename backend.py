from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, mean
from datetime import date

spark = SparkSession.builder \
    .appName("QueryBackend") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "4G") \
    .config("spark.executor.cores", "8") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

df = spark.read.json("output")
result = df
df_ner = spark.read.json("ner").toPandas()

def mean_polarity():
    global result
    return [result.agg(mean(result.sentiment_polarity)).collect()[0][0], result.agg(mean(result.sentiment_subjectivity)).collect()[0][0]]

def mean_label_polarity(aidr_labels):
    means = []

    for label in aidr_labels:
        result = df.filter(df.aidr_label == label)
        mean_polarity = result.agg(mean(result.sentiment_polarity)).collect()[0][0]
        mean_subjectivity = result.agg(mean(result.sentiment_subjectivity)).collect()[0][0]
        means.append([mean_polarity, mean_subjectivity])
    return means

def mean_state_polarity(states):
    means = []

    for state in states:
        result = df.filter(df.place_state == state)
        mean_polarity = result.agg(mean(result.sentiment_polarity)).collect()[0][0]
        mean_subjectivity = result.agg(mean(result.sentiment_subjectivity)).collect()[0][0]
        means.append([mean_polarity, mean_subjectivity])
    return means


def query(labels = None, verified = None, created_at = None, created_at_end = None, possibly_sensitive = None, place = None, phase = None):
    global df
    global result 
    df = df.filter(label_parser(labels) & verified_parser(verified) & possibly_sensitive_parser(possibly_sensitive) & date_parser(created_at, created_at_end) & place_parser(place) & phase_parser(phase))
    result = df
    return df.toPandas()

def label_parser(labels):
    return (col("aidr_label").isin(labels)) if len(labels) > 0 else lit(True)

def date_parser(created_at, created_at_end):

    if created_at_end is None:
        return (col("date") == created_at) if created_at is not None else lit(True)
    else:
        return ((col("date") >= created_at) & (col("date") <= created_at_end))

def place_parser(place):
    return( (col("place_state").isin(place) if len(place) > 0 else lit(True)))

def verified_parser(verified):
    return (col("verified") == True) if verified == "True" else (col("verified") == False) if verified == "False" else lit(True)

def possibly_sensitive_parser(ps):
     return (col("possibly_sensitive") == True) if ps == "True" else (col("possibly_sensitive") == False) if ps == "False" else lit(True)
 
def phase_parser(phase):
    
    phase_parsed = []
    for p in phase:
        if p == "Pre":
            phase_parsed.append(0)
        elif p == "Durante":
            phase_parsed.append(1)
        else:
            phase_parsed.append(2)
    return col("phase").isin(phase_parsed) if len(phase_parsed) > 0 else lit(True)
 
