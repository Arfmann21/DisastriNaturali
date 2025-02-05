from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from datetime import date

spark = SparkSession.builder.master("spark://192.168.1.13:7077") \
    .appName("QueryBackend") \
    .getOrCreate()

df = spark.read.json("output")

def scatter_plot():
    df_t = df.filter(df.place.country_code == "US")
    df_t.select("place.bounding_box.coordinates")
    #plt.scatter(df_t.place.coordinates[0], df_t.place.coordinates[1])

def query(labels = None, verified = None, created_at = None, created_at_end = None, possibly_sensitive = None, place = None, coordinates = None, groupByValue = None):
    #return spark.sql("SELECT * FROM Dataset").collect()

    '''query = "SELECT * FROM Dataset WHERE true "

    if(verified != 2): query += "AND verified = " + str(bool(verified))
    if(created_at is not None): 
        if(len(created_at) == 1):
            query += " AND createdAt = " + created_at[0]

    if(possibly_sensitive is not None): query += " AND possibly_sensitive = " + possibly_sensitive
    if(place is not None): query += " AND place = " + place
    if(coordinates is not None): query += " AND coordinates = " + coordinates
    if(label is not None and len(label) > 0):
       query += " AND aidr_label = \"" + label[0] + "\""
       
    if(groupByValue is not None): query += " GROUPBY " + groupByValue
    return spark.sql(query)

    '''

    return df.filter(label_parser(labels) & verified_parser(verified) & possibly_sensitive_parser(possibly_sensitive) & date_parser(created_at, created_at_end) & place_parser(place)).toPandas()

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
