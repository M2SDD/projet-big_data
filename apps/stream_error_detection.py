from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType

# Constantes
APP_NAME = "StreamErrorDetection"
MONGO_URI = "mongodb://mongodb-container:27017/logs_db"
COLLECTION_NAME = "stream_errors"
DELTA = 10  # delta de 10 secondes
HOST = "data-generator"
PORT = 9998

# Initialisation de la session Spark
spark = SparkSession.builder \
                    .appName(APP_NAME) \
                    .config("spark.mongodb.write.connection.uri", MONGO_URI) \
                    .getOrCreate()

# Réduire la verbosité : afficher uniquement les logs de niveau WARN et plus graves
spark.sparkContext.setLogLevel("WARN")


print(f"\nNettoyage de la collection '{COLLECTION_NAME}' dans MongoDB...")

# On crée un DataFrame vide pour déclencher le mode "overwrite"
# Cela force MongoDB à supprimer (drop) la collection existante
spark.createDataFrame([], schema=StructType([StructField("dummy", StringType(), True)])) \
     .write \
     .format("mongodb") \
     .mode("overwrite") \
     .option("collection", COLLECTION_NAME) \
     .save()


print(f"\n\nDémarrage du Streaming : {APP_NAME}")
print(f"Écoute sur {HOST}:{PORT}...")

# 1. Lecture du flux (Socket)
# Créer un DataFrame de streaming qui se connecte au générateur de données
# On récupère un DataFrame avec une seule colonne "value"
lines = spark.readStream.format("socket") \
                        .options(**{"host": HOST, "port": PORT}) \
                        .load()


# 2. Parsing via Spark SQL (Regex)
# Note : En streaming, on préfère les fonctions natives Spark (regexp_extract) 
# aux UDF Python (utils.py) pour la performance.
# Regex pour capturer : Date (grp 1) et Code (grp 2)
regex_pattern = r'\[(.*?)\] ".*?" (\d{3})'

parsed_df = lines.select(
    # Extraction du Timestamp (Format: 28/Jan/2025:15:40:01 +0000)
    to_timestamp(regexp_extract(col("value"), regex_pattern, 1), "dd/MMM/yyyy:HH:mm:ss Z").alias("timestamp"),
    # Extraction du Code
    regexp_extract(col("value"), regex_pattern, 2).alias("code")
)


# 3. Filtrage et agrégation
# On garde uniquement les codes d'erreur 404 et 500
errors_df = parsed_df.filter(col("code").isin("404", "500"))

# On définit un "Watermark" (tolérance de retard) pour gérer les données qui pourraient arriver tard (problème réseau, lag).
# Dans la réalité, un log de 15:19:50 peut arriver chez Spark à 15:20:23.
# cf https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withWatermark.html#pyspark-sql-dataframe-withwatermark
# Ici, on garde les données jusqu'à une minute après l'heure actuelle du flux
# Puis on compte par fenêtre de 10 secondes
windowed_counts = errors_df.withWatermark("timestamp", "1 minute") \
                           .groupBy(
                               window(col("timestamp"), "10 seconds"),  # regroupe tous les logs sur un intervalle de 10 s
                               col("code")
                           ) \
                           .count() \
                           .select(
                               col("window.start").alias("start_time"),
                               col("window.end").alias("end_time"),
                               col("code"),
                               col("count")
                           )


# 4. Fonction pour écrire chaque micro-batch dans MongoDB
def write_to_mongo(batch_df, batch_id):
    # On vérifie si le batch n'est pas vide pour éviter de spammer Mongo
    if not batch_df.isEmpty():
        print(f"Écriture Batch {batch_id} dans MongoDB")

        # On trie par heure de début, puis par code
        sorted_batch = batch_df.orderBy(col("start_time").asc(), col("code").asc())

        sorted_batch.show(truncate=False) # Affichage console pour debug
        
        # notre id est le tuple (start_time, end_time, code)
        # si un objet est mis à jour (données arrivées en retard) on le remplace
        sorted_batch.write \
                    .format("mongodb") \
                    .mode("append") \
                    .option("collection", COLLECTION_NAME) \
                    .option("operationType", "replace") \
                    .option("idFieldList", "start_time,end_time,code") \
                    .save()
        print(f"Batch {batch_id} écrit avec succès.")


# 5. Démarrage de la Query
# On utilise foreachBatch pour pouvoir utiliser le connecteur Mongo standard
query = windowed_counts.writeStream.format("console") \
                                   .outputMode("update") \
                                   .foreachBatch(write_to_mongo) \
                                   .start()

print("Streaming en cours... (Ctrl+C pour arrêter)")
query.awaitTermination()
