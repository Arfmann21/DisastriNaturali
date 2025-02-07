from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
from pyspark.sql.functions import hour, when, lit, split, create_map, to_date
from itertools import chain
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampNTZType
from textblob import TextBlob
from email.utils import parsedate_to_datetime
from datetime import datetime
import time


# Creare una sessione Spark
spark = SparkSession.builder.master("spark://192.168.1.13:7077") \
    .appName("FeaturesSelection") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "4G") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.executor.cores", "8") \
        .getOrCreate() \



start_time = time.time()
########### AGGIUNTA DELL'ETICHETTA DEL CLASSIFICATORE AL DATASET ###########
# Crea un RDD per aggiungere la colonna AIDRlabel, etichetta data dal classificatore. Non viene fatta automaticamente la suddivisione dei campi
rdd_classifier = spark.read.text("classification.txt").rdd

# Suddivide le stringhe che compongono i record del RDD in singoli valori per i campi
rdd_classifier_splitted = rdd_classifier.map(lambda x: x[0].split("\t"))

# Rimuove la prima riga, che corrisponde, per costruzione, ai nomi dei campi
header = rdd_classifier_splitted.first()
rdd_clean = rdd_classifier_splitted.filter(lambda line: line != header)

# Lista per definire le colonne che risulteranno nel dataframe corrispondente
columns = ['id', 'date', 'aidr_label', 'aidr_confidence']

df_c = spark.createDataFrame(rdd_clean, columns)

# Di default tutti i campi del dataframe creato da un RDD risultante di un read text sono stringhe
# Converte ID da String a Long per avere corrispondenza con quello del dataset
df_c = df_c.withColumn('id', df_c['id'].cast(LongType()))

########### CREAZIONE DEL DATASET FINALE ###########
# Carica il file JSON in un DataFrame
df = spark.read.json('dataset')

df_subset = df.select("id", "user.name", "user.screen_name", "user.verified", 
                      "text", "extended_tweet.full_text", "created_at", 
                      "retweeted_status", "truncated", "possibly_sensitive", 
                      "favorite_count", "place", "coordinates")

# Rimuove i tweet che sono retweet per diminuire di molto la dimensionalità, non danno valore aggiunto al dataset 
df_subset = df_subset.filter(df_subset['retweeted_status'].isNull())


########### CALCOLO DEL SENTIMENT CON TEXTBLOB, SEPARAZIONE DATA/ORA ###########
df_subset_list = df_subset.collect()
df_sentiment_list = [{}]

df_date = [{}]

for row in df_subset_list:
    if(row["truncated"] == True):
        sentiment = TextBlob(row["full_text"]).sentiment
    else:
        sentiment = TextBlob(row["text"]).sentiment
    
    df_sentiment_list.append({"id": row["id"], "sentiment": sentiment})
    df_date.append({"id": row["id"], "date": parsedate_to_datetime(row["created_at"])})

    if(row["id"] is None): print("None")

# Aggiunta delle colonna con i relativi valori al dataframe principale
df_sentiment = spark.createDataFrame(data = df_sentiment_list, schema = ["id", "sentiment"])

date_schema = StructType([
    StructField("id", LongType(), True),
    StructField("date", TimestampNTZType(), True)
    ]
)
df_date = spark.createDataFrame(data = df_date, schema = date_schema)

df_subset = df_subset.join(df_sentiment, "id")
df_subset = df_subset.join(df_date, "id")
df_subset = df_subset.withColumn("sentiment_polarity", df_subset.sentiment.polarity)
df_subset = df_subset.withColumn("sentiment_subjectivity", df_subset.sentiment.subjectivity)
# DataFrame con le etichette del classificatore
df_total = df_subset.join(df_c.select('id', 'aidr_label'), 'id')


########### TRASFORMAZIONE DEI CAMPI PER LA VISUALIZZAZIONE ###########

# Sostituisce il text con il full_text in caso di truncated
df_total = df_total.withColumn("text", when(df_total["truncated"] == True, df_total["full_text"]).otherwise(df_total["text"]))

# Sostituisce place con place_latitude e place_longitude per le sue coordinate e full_name per città
df_total = df_total.withColumn("place_latitude", df_total.place.bounding_box.coordinates[0][0][0])
df_total = df_total.withColumn("place_longitude", df_total.place.bounding_box.coordinates[0][0][1])
df_total = df_total.withColumn("place_name", df_total.place.full_name)

# Esplicita latitudine e longitudine del campo coordinates
df_total = df_total.withColumn("longitude", df_total.coordinates.coordinates[0])
df_total = df_total.withColumn("latitude", df_total.coordinates.coordinates[1])


########### NORMALIZZAZIONE DEL CAMPO place_name PER POTER AVERE SEMPRE L'ABBREVIAZIONE DELLO STATO

state_mapping = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY"
}

mapping_expr = create_map([lit(x) for x in chain(*state_mapping.items())])

df_total = df_total.withColumn("place_state", when((df_total.place.country_code == "US") & (df_total.place.place_type == "admin"), mapping_expr[split(df_total.place_name, ", ")[0]])
                               .otherwise(when((df_total.place.country_code == "US") & (df_total.place.place_type == "city"), split(df_total.place_name, ", ")[1]))) 

df_total = df_total.withColumn("phase", when(df_total.date < to_date(lit("2017-08-17")), lit(0)).when( (df_total.date >= to_date(lit("2017-08-17"))) & (df_total.date <= to_date(lit("2017-09-12")) ), lit(1)).otherwise(lit(2)))

df_total = df_total.withColumn("timerange_end", hour(df_total.date) + 1)

# Drop di colonne non rilevanti
columns_to_drop = ["id", "full_text", "truncated", "coordinates", "sentiment", "place", "created_at"]
df_total = df_total.drop(*columns_to_drop)

########### ###########

# Scrive il dataset in file JSON
df_total.write.json("output", "overwrite")

end_time = time.time()

print(f"Tempo di esecuzione: {end_time - start_time:.2f} secondi")