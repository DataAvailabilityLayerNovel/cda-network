#!/bin/bash
# cleanup.sh - Stop containers and delete host data files

echo "[*] Stopping docker compose containers and removing volumes..."
docker compose -f docker-compose.json down -v --remove-orphans

echo "[*] Cleaning up local data directories..."
# Keep prometheus and grafana configs, but clear store databases, light logs, and completion logs
if [ -d "data" ]; then
    find data/ -maxdepth 1 -name "store_*" -exec sudo rm -rf {} +
    find data/ -maxdepth 1 -name "light_*" -exec sudo rm -rf {} +
    sudo rm -f data/completion.log
fi

echo "[+] Cleanup completed successfully."
