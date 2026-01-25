#!/bin/bash
docker compose down
echo -e "\n"
docker volume prune -f
echo -e "\n\nSupression du volume de MongoDB :"
docker volume rm projet_mongodb_data