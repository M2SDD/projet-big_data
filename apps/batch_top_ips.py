from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from utils import parse_log_line

# Constantes
APP_NAME = "AnalyzeLogsTopIPs"
MONGO_URI = "mongodb://mongodb-container:27017/logs_db"
HDFS_PATH = "hdfs://hdfs-namenode:9000/logs/web_server.log"
COLLECTION_NAME = "batch_top_ips"

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


# 3. MapReduce : Comptage par IP
# Etape Map : (IP, 1)
# Etape Reduce : Somme des 1 par IP
counts_rdd = parsed_logs_rdd.map(lambda log: (log['ip'], 1)) \
                            .reduceByKey(lambda a, b: a + b)


# 4. Identification du Top 10
# -x[1] pour trier par le nombre (index 1) en ordre décroissant (négatif)
top_10_ips = counts_rdd.takeOrdered(10, key=lambda x: -x[1])


# 5. Préparation pour MongoDB
# Conversion du RDD final en DataFrame pour faciliter l'écriture Mongo
schema = StructType([
    StructField("ip", StringType(), False),
    StructField("count", IntegerType(), False)
])

# On mappe le résultat (ip, count) vers le schéma
df_result = spark.createDataFrame(top_10_ips, schema)


# 6. Affichage console
print("Résultats : Top 10 des adresses IP les plus actives")
df_result.show(truncate=False)


# 7. Sauvegarder les résultats dans MongoDB
print(f"Sauvegarde dans la collection '{COLLECTION_NAME}'")
df_result.write \
         .format("mongodb") \
         .mode("overwrite") \
         .option("collection", COLLECTION_NAME) \
         .save()

print("Traitement terminé avec succès.")
spark.stop()