from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import col, round
from utils import parse_log_line

# Constantes
APP_NAME = "AnalyzeLogsCodesHTTP"
MONGO_URI = "mongodb://mongodb-container:27017/logs_db"
HDFS_PATH = "hdfs://hdfs-namenode:9000/logs/web_server.log"
COLLECTION_NAME = "batch_http_codes"

# Initialisation de la session Spark
spark = SparkSession.builder \
                    .appName(APP_NAME) \
                    .config("spark.mongodb.write.connection.uri", MONGO_URI) \
                    .getOrCreate()

# Initialiser le contexte Spark
sc = spark.sparkContext

# Réduire la verbosité : afficher uniquement les logs de niveau WARN et plus graves
sc.setLogLevel("WARN")

print(f"\n\nDémarrage du traitement Batch : {APP_NAME}")


# 1. Chargement des fichiers depuis HDFS
logs_rdd = sc.textFile(HDFS_PATH)


# 2. Parsing et filtrage
parsed_logs_rdd = logs_rdd.map(parse_log_line) \
                          .filter(lambda x: x is not None)

# On met en cache car on va utiliser le RDD deux fois : compte total et MapReduce
parsed_logs_rdd.cache()

# Calcul du nombre total de requêtes pour la fréquence
total_requests = parsed_logs_rdd.count()
print(f"Total des requêtes analysées : {total_requests}")


# 3. MapReduce : comptage des codes
# Etape Map : transforme chaque log en une paire (code, 1)
# Etape Reduce : somme les 1 pour chaque clé (code)
counts_rdd = parsed_logs_rdd.map(lambda log: (log['code'], 1)) \
                            .reduceByKey(lambda a, b: a + b)


# 4. Préparation pour MongoDB
# Conversion du RDD final en DataFrame pour faciliter l'écriture Mongo
schema = StructType([
    StructField("code", IntegerType(), False),
    StructField("count", IntegerType(), False)
])

# On mappe le résultat (code, count) vers le schéma
df_counts = spark.createDataFrame(counts_rdd, schema)


# 5. Enrichissement avec descriptions et pourcentages
# Création d'un petit DataFrame de référence pour les descriptions
codes_data = [
    (200, "Succès de la requête"),
    (404, "Ressource non trouvée"),
    (301, "Redirection"),
    (500, "Erreur interne du serveur"),
    (403, "Accès refusé")
]
df_desc = spark.createDataFrame(codes_data, ["code", "description"])

# Jointure pour ajouter la description
df_enriched = df_counts.join(df_desc, on="code", how="left")

# Calcul du pourcentage
df_result = df_enriched.withColumn("frequence", round((col("count") / total_requests), 4))

# Remplacer les nulls par "Autre code" si jamais il y a un code inconnu
df_result = df_result.na.fill({"description": "Autre code"})


# 6. Affichage console
print("Résultats : Fréquence des codes HTTP")
df_result.orderBy("count", ascending=False) \
         .show(truncate=False)


# 7. Sauvegarder les résultats dans MongoDB
print(f"Sauvegarde dans la collection '{COLLECTION_NAME}'")
df_result.write \
         .format("mongodb") \
         .mode("overwrite") \
         .option("collection", COLLECTION_NAME) \
         .save()

print("Traitement terminé avec succès.")
spark.stop()