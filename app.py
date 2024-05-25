import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
from pyspark.sql.functions import col, count

# Création de la session Spark
spark = (SparkSession
         .builder
         .master('local')
         .appName('word-count')
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1")
         .getOrCreate())

# Lire le flux de Kafka
kafka_df = (spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "webscraping-data")
            .option("startingOffsets", "earliest")
            .load())

# Sélectionner la colonne de valeur et la convertir en chaîne de caractères
lines = kafka_df.selectExpr("CAST(value AS STRING)")

# Diviser les lignes en mots
words = lines.select(explode(split(col("value"), " ")).alias("word"))

# Calculer le nombre de mots
word_counts = words.groupBy("word").count()

# Démarrer la requête en écrivant les résultats dans la console
query = (word_counts
         .writeStream
         .outputMode("complete")
         .format("console")
         .start())

# Attendre la fin de la requête
query.awaitTermination()
import streamlit as st
import time


def load_data():
    # Read the data from the memory sink
    return word_counts.sql("SELECT * FROM word_counts")

# Use the st.empty function to create a placeholder for the data
table_placeholder = st.empty()

# Update the data every second
while True:
    data = load_data()
    table_placeholder.table(data.toPandas())
    time.sleep(1)