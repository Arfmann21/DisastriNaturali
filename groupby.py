from pyspark.sql import SparkSession
import pyspark.sql.functions as F 

spark = SparkSession.builder \
    .appName("TweetNER") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.executor.cores", "8") \
    .getOrCreate()

df = spark.read.json("ner_entity_counts")

entity_counts_df = df.groupBy("entity", "type").agg(
    F.sum("count").alias("total_count")
).orderBy(F.col("total_count").desc())

entity_counts_df.write.json("final_ner_entity_counts", mode="overwrite")

entity_counts_df.show(truncate=False)

print("Group By completed successfully!")
