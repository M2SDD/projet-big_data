#!/bin/bash
# construction de l'image apache-spark:3.4.0
docker build -t apache-spark:3.4.0 .
# instanciation et orchestration des containers
docker compose up -d
echo -e "\n"
# suivit de l'envoi des fichiers logs sur HDFS
docker logs -f hdfs-client
echo -e "\nFin de l'installation."