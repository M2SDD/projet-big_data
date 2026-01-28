#!/bin/bash
# destruction des containers
docker compose down
echo -e "\n"
# suppression des volumes non utilisés (volumes spark)
docker volume prune -f
# suppression du volume nommé de mongodb
echo -e "\n\nSupression du volume de MongoDB :"
docker volume rm projet-big-data_mongodb_data