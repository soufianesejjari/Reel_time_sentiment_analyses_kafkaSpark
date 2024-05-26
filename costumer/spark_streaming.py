import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, StructType, StructField
from textblob import TextBlob
from nltk.corpus import stopwords
import re

# Télécharger les stop words de NLTK (à exécuter une fois en local)
import nltk
nltk.download('stopwords')

# Initialiser la session Spark
spark = (SparkSession
         .builder
         .master('local[*]')
         .appName('YouTubeCommentsProcessing')
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
         .getOrCreate())

# Liste des stop words
stop_words = set(stopwords.words('english'))

# Fonction de nettoyage des commentaires
def clean_comment(comment):
    # Convertir en minuscules
    comment = comment.lower()
    # Supprimer les hashtags et les mentions
    comment = re.sub(r'@\w+|#\w+', '', comment)
    # Supprimer les caractères non alphabétiques
    comment = re.sub(r'[^a-z\s]', '', comment)
    # Supprimer les stop words
    words = comment.split()
    filtered_words = [word for word in words if word not in stop_words]
    cleaned_comment = ' '.join(filtered_words)
    return cleaned_comment

# UDF (User Defined Function) pour nettoyer les commentaires
clean_comment_udf = udf(clean_comment, StringType())

# Schéma des données Kafka
schema = StructType([
    StructField("source", StringType(), True),
    StructField("date", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("topic", StringType(), True)
])

# Lire le flux de Kafka
kafka_df = (spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "test-sentiments")
            .option("startingOffsets", "latest")  # Lire uniquement les nouveaux messages
            .load())

# Convertir les données Kafka en DataFrame avec le schéma défini
json_df = kafka_df.selectExpr("CAST(value AS STRING)").select(F.from_json("value", schema).alias("data")).select("data.*")

# Nettoyer les commentaires
cleaned_df = json_df.withColumn("cleaned_comment", clean_comment_udf(col("comment")))

# Fonction UDF pour analyser les sentiments avec TextBlob
def analyze_sentiment(comment):
    analysis = TextBlob(comment)
    if analysis.sentiment.polarity > 0:
        return 'POSITIVE'
    elif analysis.sentiment.polarity == 0:
        return 'NEUTRAL'
    else:
        return 'NEGATIVE'

sentiment_udf = udf(analyze_sentiment, StringType())

# Appliquer l'analyse des sentiments
sentiment_df = cleaned_df.withColumn("sentiment", sentiment_udf(col("cleaned_comment")))

# Fonction pour écrire dans MongoDB
def write_mongodb(df, epoch_id):
    df.write.format("mongo").mode("append").option("uri", "mongodb://localhost:27017/mydatabase.mycollection").save()

# Démarrer la requête en écrivant les résultats dans MongoDB
query = (sentiment_df
         .writeStream
         .outputMode("append")  # Utiliser 'append' pour ajouter continuellement de nouvelles lignes
         .foreachBatch(write_mongodb)
         .start())

# Attendre la fin de la requête
query.awaitTermination()

