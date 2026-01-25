#!/bin/bash
docker compose up -d
echo -e "\n"
docker logs -f hdfs-client
echo -e "\nFin de l'installation."