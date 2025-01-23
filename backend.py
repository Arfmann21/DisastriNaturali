from pyspark.sql import SparkSession

spark = SparkSession.builder.master("spark://192.168.1.13:7077") \
    .appName("QueryBackend") \
    .getOrCreate()

df = spark.read.json("output").createOrReplaceTempView("Dataset")

def query(label = None, username = None, verified = None, created_at = None, possibly_sensitive = None, place = None, coordinates = None, groupByValue = None):
    #return spark.sql("SELECT * FROM Dataset").collect()

    query = "SELECT * FROM Dataset WHERE true"

    if(verified is not None): query += "AND verified = " + verified
    if(created_at is not None): query += " AND createdAt = " + created_at
    if(possibly_sensitive is not None): query += " AND possibly_sensitive = " + possibly_sensitive
    if(place is not None): query += " AND place = " + place
    if(coordinates is not None): query += " AND coordinates = " + coordinates
    if(label is not None):
       query += " AND aidr_label = \"" + label[0] + "\""
       
    if(groupByValue is not None): query += " GROUPBY " + groupByValue
    return spark.sql(query).collect()
