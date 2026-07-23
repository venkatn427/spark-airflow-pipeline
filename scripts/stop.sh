#!/usr/bin/env bash
# scripts/stop.sh  –  graceful teardown

DC="docker compose"
command -v docker-compose >/dev/null 2>&1 && DC="docker-compose"

echo "Stopping all pipeline services…"
$DC down --remove-orphans

echo "Done. To also remove volumes (all data): docker compose down -v"
