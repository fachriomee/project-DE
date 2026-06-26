#!/bin/bash

chmod +x run_airflow.sh

docker compose build --no-cache
docker compose up -d

sleep 10

docker compose --profile flower up -d

echo "Current dir is $PWD"