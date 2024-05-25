from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

# Initialiser la session Spark
spark = SparkSession.builder \
    .appName("YouTubeCommentsProcessing") \
    .getOrCreate()

# Initialiser le contexte de streaming avec un intervalle de 60 secondes
ssc = StreamingContext(spark.sparkContext, 60)

# Configurer Kafka
kafka_brokers = "localhost:9092"
kafka_topic = "webscraping-data"

# Créer un flux de Kafka
kafka_stream = KafkaUtils.createDirectStream(ssc, [kafka_topic], {"metadata.broker.list": kafka_brokers})

# Fonction de nettoyage des commentaires
def clean_comment(comment):
    # Exemples de nettoyage : suppression des espaces superflus, conversion en minuscule
    cleaned_comment = comment.strip().lower()
    return cleaned_comment

# UDF (User Defined Function) pour nettoyer les commentaires
clean_comment_udf = udf(clean_comment, StringType())

# Fonction de traitement des RDDs
def process_rdd(rdd):
    if not rdd.isEmpty():
        # Convertir l'RDD en DataFrame
        df = spark.read.json(rdd.map(lambda x: x[1]))

        # Nettoyer les commentaires
        df_cleaned = df.withColumn("cleaned_comment", clean_comment_udf(col("comment")))

        # Sélectionner les colonnes pertinentes
        df_result = df_cleaned.select("commentDate", "cleaned_comment", "source")

        # Afficher le DataFrame nettoyé
        df_result.show()

# Traiter les messages de Kafka
kafka_stream.foreachRDD(process_rdd)

# Démarrer le contexte de streaming
ssc.start()
ssc.awaitTermination()
