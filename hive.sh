#!/usr/bin/env sh

# Download the container
export HIVE_VERSION=4.0.1
docker pull apache/hive:$HIVE_VERSION

if docker ps | grep -i hive4; then
  # If container is already running, do nothing
  exit 0
elif docker ps -a | grep -i hive4; then
  # If container is stopped, remove it
  docker rm hive4
fi

docker run -d \
  -p 10000:10000 \
  -p 10002:10002 \
  -v "$(pwd -P)/data:/data" \
  --memory="4g" \
  --cpus="4" \
  --env SERVICE_NAME=hiveserver2 \
  --env HIVE_HEAP_MEMORY=3072 \
  --name hive4 \
  apache/hive:$HIVE_VERSION
