# Projet Architecture Distribuée : Analyse de Logs Web

Ce projet met en œuvre une architecture Big Data distribuée pour l'analyse de logs de serveur web (site e-commerce). Il combine des traitements **Batch** (sur données historiques) et **Streaming** (analyse temps réel) utilisant Hadoop HDFS, Apache Spark et MongoDB, le tout conteneurisé avec Docker.

## 📂 Structure du Projet

```text
/projet-big_data
  ├── docker-compose.yml             # Orchestration des conteneurs (Hadoop, Spark, Mongo)
  ├── install.sh                     # Script de lancement et d'initialisation
  ├── uninstall.sh                   # Script d'arrêt et de nettoyage
  ├── README.md                      # Documentation du projet
  ├── data/
  │    └── web_server.log            # Dataset brut (Logs)
  ├── apps/                          # Scripts PySpark
  │    ├── utils.py                  # Fonctions utilitaires (parsing)
  │    ├── batch_http_codes.py       # Analyse Batch : Codes HTTP
  │    ├── batch_top_ips.py          # Analyse Batch : Top IPs
  │    └── stream_error_detection.py # Analyse Stream : Détection d'erreurs
  └── data_generator/
       └── data_generator.py         # Simulateur de flux de logs (TCP Socket)
```

## 🏗️ Architecture

L'infrastructure déployée via `docker-compose` comprend:

- **Stockage Distribué (HDFS) :** Un cluster Hadoop avec 1 NameNode, 1 Secondary NameNode et 2 DataNodes pour stocker les logs statiques. Il comprend aussi un client dont le rôle et de téléverser le fichier `web_server.log` sur HDFS. Une fois le fichier téléversé, le client s'arrête.


- **Traitement (Spark) :** Un cluster Spark (1 Master, 2 Workers) pour l'exécution des calculs distribués.


- **Base de Données (MongoDB) :** Stockage NoSQL pour la persistance des résultats d'analyse.


- **Ingestion (Data Generator) :** Un service Python simulant un flux de logs en temps réel via socket TCP.

## 🚀 Installation

### Prérequis

* Docker & Docker Compose installés sur la machine.
* Le fichier `web_server.log` doit être présent dans le dossier `data/` du projet.

### Lancement Rapide

Placez-vous dans le dossier du projet et utilisez le script d'installation fourni. Il lance les conteneurs et charge automatiquement les données dans HDFS.

```bash
./install.sh
```

> **Note :** Le script affiche les logs du client HDFS. Attendez de voir le message `[HDFS-Client] Fichier web_server.log uploadé avec succès sur HDFS !` avant de lancer les analyses.

Vous pouvez bien sûr faire un `docker compose up -d`, mais je ne saurais trop vous conseiller de monitorer ensuite l'envoi du fichier par le client à l'aide de `docker logs -f hdfs-client` avant de commencer toute analyse.

Pour vérifier que le fichier `web_server.log` est bien sur HDFS, entrez la commande :

```bash
docker exec -it namenode hdfs dfs -ls /data
```

---

## 📊 Exécution des analyses

Toutes les commandes s'exécutent depuis le terminal de votre machine hôte via `docker exec`.

### 1. Analyses en batch

Ces jobs lisent le fichier stocké sur HDFS (`hdfs://namenode:9000/logs/web_server.log`).

**Analyse A : Répartition des codes HTTP**  
Calcule la fréquence des codes HTTP (200, 404, 500...).

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 \
  --py-files /opt/spark-apps/utils.py \
  --master spark://spark-master:7077 \
  /opt/spark-apps/batch_http_codes.py
```

**Analyse B : Top 10 Adresses IP**  
Identifie les utilisateurs les plus actifs sur le site.

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 \
  --py-files /opt/spark-apps/utils.py \
  --master spark://spark-master:7077 \
  /opt/spark-apps/batch_top_ips.py
```

### 2. Analyse en streaming

Ce job écoute le `data-generator` sur le port 9998 et détecte les pics d'erreurs (404/500) par fenêtre de temps de 10 secondes.

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 \
  --master spark://spark-master:7077 \
  /opt/spark-apps/stream_error_detection.py
```
Vous pouvez en même temps consulter les logs du `data-generator` via la commande :

```bash
docker logs -f data-generator
```

*(Utilisez `Ctrl+C` pour arrêter le streaming, ou pour sortir des logs du `data-generator`)*.

> **Note :** Si la connexion est interrompue entre Spark et le `data-generator`, ce dernier redémarrera après une seconde.   
> Vous pouvez d'ailleurs tester le générateur dans votre terminal à l'aide de la commande `nc localhost 9998` sous UNIX ou `telnet localhost 9998` sur PowerShell.

---

## 🔍 Consultation des résultats (MongoDB)

Les résultats sont stockés dans la base de données `logs_db`. Vous pouvez les vérifier en ligne de commande.

**Connexion au conteneur MongoDB :**

```bash
docker exec -it mongodb-container mongosh logs_db
```

**Commandes Mongo utiles :**

```javascript
// Voir les tables générées
show collections

// Voir les résultats des codes HTTP (Batch)
db.batch_http_codes.find()

// Voir le top des IPs (Batch)
db.batch_top_ips.find()

// Voir les erreurs détectées en temps réel (Stream)
db.stream_errors.find()
```

## 🛑 Arrêt et nettoyage

Pour arrêter l'architecture et supprimer les volumes (nettoyage complet des données), utilisez le script de désinstallation :

```bash
./uninstall.sh
```
Attention, ce script supprimera tous les volumes n'étant pas rattaché à un conteneur mais également le volume nommé de MongoDB.
Si vous souhaitez conserver les volumes, effectuez juste un :

```bash
docker compose down
```
