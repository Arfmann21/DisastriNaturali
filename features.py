from pyspark.sql import SparkSession
import logging
import pandas as pd

# Creare una sessione Spark
spark = SparkSession.builder \
    .appName("FeaturesSelection") \
    .getOrCreate()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SparkLogger")

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

from pyspark.sql.types import LongType

df_c = spark.createDataFrame(rdd_clean, columns)

# Di default tutti i campi del dataframe creato da un RDD risultante di un read text sono stringhe
# Converte ID da String a Long per avere corrispondenza con quello del dataset
df_c = df_c.withColumn('id', df_c['id'].cast(LongType()))

########### CREAZIONE DEL DATASET FINALE ###########
# Carica il file JSON in un DataFrame
df = spark.read.json('dataset')

df_subset = df.select("id", "user.name", "user.screen_name", "user.verified", "text", "extended_tweet.full_text", "created_at", "retweeted_status", "truncated", "possibly_sensitive", "favorite_count", "place", "coordinates", "user.location")

# Rimuove i tweet che sono retweet per diminuire di molto la dimensionalità, non danno valore aggiunto al dataset 
df_subset = df_subset.filter(df_subset['retweeted_status'].isNull())

df_total = df_subset.join(df_c.select('id', 'aidr_label'), 'id')

########### ###########

# Scrive il dataset in file JSON
df_total.write.json("output", "overwrite")

#logging.info("PRIMI 20 RECORD") 
#logging.info(df_total.show(truncate = False))

#logging.info("SCHEMA DEL DATAFARAME")
#logging.info(df_total.printSchema())

#logging.info(df_c.show(truncate = False))
#df_c.printSchema()

