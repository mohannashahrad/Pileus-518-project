#!/bin/bash

CONTAINER_NAME="redis"

if ! sudo docker ps | grep -q "$CONTAINER_NAME"; then
  echo "Redis container '$CONTAINER_NAME' not running."
  exit 1
fi

echo "Flushing all keys in Redis container '$CONTAINER_NAME'..."
sudo docker exec -it "$CONTAINER_NAME" redis-cli FLUSHALL

echo "Done."
