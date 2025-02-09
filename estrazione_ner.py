from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, explode, when
from pyspark.sql.types import ArrayType, StructType, StructField, StringType
import pandas as pd

from flair.data import Sentence
from flair.models import SequenceTagger

tagger = SequenceTagger.load("ner-fast")

spark = SparkSession.builder \
    .appName("TweetNER") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.executor.cores", "8") \
    .getOrCreate()

df = spark.read.json("output")

df = df.select('text').filter(col('text').isNotNull())

output_schema = ArrayType(StructType([
    StructField("entity", StringType(), True),
    StructField("type", StringType(), True)
]))

@pandas_udf(output_schema)
def extract_entities(text_series: pd.Series) -> pd.Series:
    results = []
    for text in text_series:
        sentence = Sentence(text)
        tagger.predict(sentence)
        entities = []
        for entity in sentence.get_spans("ner"):
            entities.append({
                "entity": entity.text,
                "type": entity.get_label("ner").value
            })
        results.append(entities)
    return pd.Series(results)

df_with_entities = df.withColumn("entities", extract_entities(col("text")))

entities_df = df_with_entities.select(explode(col("entities")).alias("entity_info"))
entities_df = entities_df.select(
    col("entity_info.entity").alias("Entity"),
    col("entity_info.type").alias("Type")
)

entities_df = entities_df.withColumn(
    "Entity", 
    when(col("Entity").rlike("(?i)^(harvey|hurricane harvey)$"), "Harvey")
    .when(col("Entity").rlike("(?i)^irma$"), "Irma")
    .otherwise(col("Entity"))
).withColumn(
    "Type", 
    when(col("Entity") == "Harvey", "MISC")
    .when(col("Entity") == "Irma", "MISC")
    .otherwise(col("Type"))
)

entity_counts_df = entities_df.groupBy("Entity", "Type").count()

entity_counts_df.write.json("named_entity_recognition_output_finale", mode="overwrite")

df_with_entities.show(truncate=False)
entities_df.show(truncate=False)
entity_counts_df.show(truncate=False)
print("Salvataggio completato in formato JSON!")
