from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
from textblob import TextBlob

# Creare una sessione Spark
spark = SparkSession.builder \
    .appName("FeaturesSelection") \
    .getOrCreate()

def clustering():
    df = spark.read("")

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

df_subset = df.select("id", "user.name", "user.screen_name", "user.verified", "text", "extended_tweet.full_text", "created_at", "retweeted_status", "truncated", "possibly_sensitive", "favorite_count", "place", "coordinates", "user.location")

# Rimuove i tweet che sono retweet per diminuire di molto la dimensionalità, non danno valore aggiunto al dataset 
df_subset = df_subset.filter(df_subset['retweeted_status'].isNull())


########### CALCOLO DEL SENTIMENT CON TEXTBLOB ###########
df_subset_list = df_subset.collect()
df_sentiment_list = [{}]

for row in df_subset_list:
    if(row["truncated"] == True):
        sentiment = TextBlob(row["full_text"]).sentiment
    else:
        sentiment = TextBlob(row["text"]).sentiment
    
    df_sentiment_list.append({"id": row["id"], "sentiment": sentiment})

# Aggiunta della colonna con i relativi valori al dataframe principale
df_sentiment = spark.createDataFrame(data = df_sentiment_list, schema = ["id", "sentiment"])
df_subset = df_subset.join(df_sentiment, "id")

# DataFrame finale con le etichette del classificatore
df_total = df_subset.join(df_c.select('id', 'aidr_label'), 'id')


########### ###########

# Scrive il dataset in file JSON
df_total.write.json("output", "overwrite")
