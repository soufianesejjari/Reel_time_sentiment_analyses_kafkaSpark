import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, StructType, StructField
from kafka import KafkaProducer

# Initialiser la session Spark
spark = (SparkSession
         .builder
         .master('local[*]')
         .appName('YouTubeCommentsProcessing')
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
         .getOrCreate())

# Fonction de nettoyage des commentaires
def clean_comment(comment):
    cleaned_comment = comment.strip().lower()
    return cleaned_comment

# UDF (User Defined Function) pour nettoyer les commentaires
clean_comment_udf = udf(clean_comment, StringType())
# Schéma des données Kafka
schema = StructType([
    StructField("source", StringType(), True),
    StructField("date", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("sentiment", StringType(), True)
])

# Lire le flux de Kafka
kafka_df = (spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "test-sentiments")
            .option("startingOffsets", "latest")  # Read only the new messages
            .load())

# Convertir les données Kafka en DataFrame avec le schéma défini
json_df = kafka_df.selectExpr("CAST(value AS STRING)").select(F.from_json("value", schema).alias("data")).select("data.*")

# Nettoyer les commentaires
cleaned_df = json_df.withColumn("cleaned_comment", clean_comment_udf(col("comment")))
def write_mongodb(df, epoch_id):
    df.write.format("mongo").mode("append").option("uri", "mongodb://localhost:27017/mydatabase.mycollection").save()

# Démarrer la requête en écrivant les résultats dans la console
query = (cleaned_df
         .writeStream
         .outputMode("append")  # Utiliser 'append' pour ajouter continuellement de nouvelles lignes
                  .foreachBatch(write_mongodb)
         .start())

# Attendre la fin de la requête
query.awaitTermination()
