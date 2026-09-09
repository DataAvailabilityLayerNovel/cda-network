#!/bin/bash
# cleanup.sh - Stop containers and delete host data files

# 1. Stop native background test processes (if running)
echo "[*] Terminating native CDA node processes..."
pkill -9 -f "bin/publisher|bin/bootstrap|bin/store|bin/light" 2>/dev/null || true

# 2. Stop docker compose containers (if running and docker daemon is active)
if command -v docker >/dev/null 2>&1 && timeout 1s docker info >/dev/null 2>&1 && [ -f "docker-compose.json" ]; then
    echo "[*] Stopping docker compose containers and removing volumes..."
    timeout 5s docker compose -f docker-compose.json down -v --remove-orphans 2>/dev/null || true
fi

# 3. Clean up local databases, logs, identity keys, and build artifacts
echo "[*] Cleaning up local data directories, databases, logs, and store keys..."
rm -rf data/store_* data/light_* data/publisher data/bootstrap_*
rm -f publisher.log light*.log bootstrap*.log store*.log data/completion.log
rm -f store_*.key *.key
rm -rf bin/

echo "[+] Cleanup completed successfully."
