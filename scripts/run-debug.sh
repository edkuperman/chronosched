#!/usr/bin/env bash
echo "Starting Chronosched in DEBUG mode..."
docker compose -f docker-compose.yml -f docker-compose.debug.yml up --build
