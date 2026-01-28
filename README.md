# Projet Architecture Distribuée : Analyse de Logs Web

Ce projet met en œuvre une architecture Big Data distribuée pour l'analyse de logs de serveur web (site e-commerce). Il combine des traitements **Batch** (sur données historiques) et **Streaming** (analyse temps réel) utilisant Hadoop HDFS, Apache Spark et MongoDB, le tout conteneurisé avec Docker.

## 📂 Structure du Projet

```text
/projet-big_data
  ├── Dockerfile                     # Dockerfile de l'image apache-spark:3.4.0 utilisée ici
  ├── start-spark.sh                 # Script de démarage de spark utilisé dans le Dockerfile
  ├── start-defaults.conf            # Fichier de configuration pour le connecteur Spark-Mongo
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

* [Docker](https://www.docker.com/products/docker-desktop/) & Docker Compose installés sur la machine hôte.
* [Git LFS](https://git-lfs.com/) installé et initialisé avec git pour clôner le dépôt du projet avec le fichier `web_server.log`.
* Assurez vous que le fichier `web_server.log` est présent dans le dossier `data/` du projet. S'il n'est pas présent ou ne pèse pas 400 Mo environ, téléchargez le manuellement.

> **Note :** Si vous êtes sous Windows, je vous conseille vivement d'utiliser le [WSL](https://learn.microsoft.com/fr-fr/windows/wsl/).

### Lancement Rapide

Clonez ce répertoire l'aide de la commande :

```bash
git lfs clone https://github.com/M2SDD/projet-big_data.git
```

Placez-vous dans le dossier du projet et utilisez le script d'installation fourni. Il construit l'image de apache-spark:3.4.0, lance les conteneurs et charge automatiquement les données dans HDFS.

```bash
cd projet-big_data
./install.sh
```

> **Note :** Le script affiche les logs du client HDFS. Attendez de voir le message `[HDFS-Client] Fichier web_server.log uploadé avec succès sur HDFS !` avant de lancer les analyses.

Vous pouvez bien sûr faire un `docker compose up -d`, mais vous devrez avant tout reconstruire l'image d'`apache-spark:3.4.0` utilisée ici à l'aide de la commande `docker build -t apache-spark:3.4.0 .`. Je ne saurais trop vous conseiller ensuite de monitorer l'envoi du fichier par le client à l'aide de `docker logs -f hdfs-client` avant de commencer toute analyse. Soit, dans l'ordre :

```bash
docker build -t apache-spark:3.4.0 .
docker compose up -d
docker logs -f hdfs-client
```

Pour vérifier que le fichier `web_server.log` est bien sur HDFS (dans un dossier `logs` situé à la racine) entrez la commande :

```bash
docker exec -it namenode hdfs dfs -ls /logs
```

---

## 📊 Exécution des analyses

Toutes les commandes s'exécutent depuis le terminal de votre machine hôte via `docker exec`.

### 1. Analyses en batch

Ces jobs lisent le fichier stocké sur HDFS (`hdfs://namenode:9000/logs/web_server.log`).

**Analyse A : Répartition des codes HTTP**  
Calcule la fréquence des codes HTTP (200, 404, 500...).

```bash
docker exec -it spark-master spark-submit \
  --py-files /opt/spark-apps/utils.py \
  --master spark://spark-master:7077 \
  --name AnalyzeLogsCodesHTTP \
  /opt/spark-apps/batch_http_codes.py
```

**Analyse B : Top 10 Adresses IP**  
Identifie les utilisateurs les plus actifs sur le site.

```bash
docker exec -it spark-master spark-submit \
  --py-files /opt/spark-apps/utils.py \
  --master spark://spark-master:7077 \
  --name AnalyzeLogsTopIPs \
  /opt/spark-apps/batch_top_ips.py
```

### 2. Analyse en streaming

Ce job écoute le `data-generator` sur le port 9998 et détecte les pics d'erreurs (404/500) par fenêtre de temps de 10 secondes.

```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --name StreamErrorDetection \
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

// Quitter le shell Mongo (ou Ctrl+D)
exit()
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
