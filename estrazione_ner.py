from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import StructType, StructField, StringType
from flair.data import Sentence
from flair.models import SequenceTagger
import json

# Initialize Spark Session with optimized configuration
spark = SparkSession.builder \
    .appName("TweetNER") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.executor.cores", "8") \
    .getOrCreate()

# Load the Flair NER model once per worker
tagger = SequenceTagger.load("ner-fast")

# Load the processed tweet dataset
df = spark.read.json("output")

# Convert DataFrame to RDD for custom processing
rdd = df.select('text').filter(col('text').isNotNull()).rdd

# Define a function to extract named entities using Flair
def extract_entities(text):
    sentence = Sentence(text)
    tagger.predict(sentence)

    entities = [(entity.text, entity.get_label("ner").value) for entity in sentence.get_spans("ner")]
    return entities if entities else [(None, None)]

# Apply the function to the RDD
entities_rdd = rdd.flatMap(lambda row: extract_entities(row['text']))

# Convert back to DataFrame for easier handling
schema = StructType([
    StructField("entity", StringType(), True),
    StructField("type", StringType(), True)
])

entities_df = spark.createDataFrame(entities_rdd, schema)

# Normalize specific entity names
entities_df = entities_df.withColumn(
    "entity", 
    when(col("entity").rlike("(?i)^(harvey|hurricane harvey)$"), lit("Harvey"))
    .when(col("entity").rlike("(?i)^irma$"), lit("Irma"))
    .otherwise(col("entity"))
).withColumn(
    "type", 
    when(col("entity") == "Harvey", lit("MISC"))
    .when(col("entity") == "Irma", lit("MISC"))
    .otherwise(col("type"))
)

# Aggregate and count entity occurrences
entity_counts_df = entities_df.groupBy("entity", "type").count().orderBy(col("count").desc())

# Save the NER results and counts in JSON format
entities_df.write.json("ner_extracted_entities_rdd", mode="overwrite")
entity_counts_df.write.json("ner_entity_counts_rdd", mode="overwrite")

# Display results for verification
entities_df.show(truncate=False)
entity_counts_df.show(truncate=False)

print("Named Entity Recognition completed successfully!")
