#!/bin/bash
# cleanup.sh - Stop containers and delete host data files

# 1. Stop native background test processes (if running)
echo "[*] Terminating native CDA node processes..."
pkill -9 -f "bin/publisher|bin/bootstrap|bin/store|bin/light" 2>/dev/null || true

# 2. Stop docker compose containers (if docker is available)
if command -v docker >/dev/null 2>&1; then
    echo "[*] Stopping docker compose containers and removing volumes..."
    
    # Dùng docker compose down với -t 1 (tắt nhanh sau 1 giây thay vì đợi 10 giây mặc định)
    if [ -f "docker-compose.json" ]; then
        docker compose -f docker-compose.json down -v -t 1 --remove-orphans 2>/dev/null || true
    elif [ -f "docker-compose.yml" ]; then
        docker compose down -v -t 1 --remove-orphans 2>/dev/null || true
    fi

    # Dự phòng dọn dẹp sạch sẽ: cưỡng chế gỡ các container sót lại của riêng cda-network (nếu có)
    # Tuyệt đối không ảnh hưởng đến các container khác trên hệ thống (Celestia, Engram, v.v.)
    CDA_CONTAINERS=$(docker ps -aq --filter "name=cda-network" 2>/dev/null || true)
    if [ -n "$CDA_CONTAINERS" ]; then
        echo "[*] Force removing remaining CDA containers..."
        echo "$CDA_CONTAINERS" | xargs -r docker rm -f >/dev/null 2>&1 || true
    fi
fi

# 3. Clean up local databases, logs, identity keys, and build artifacts
echo "[*] Cleaning up local data directories, databases, logs, and store keys..."
if sudo -n true 2>/dev/null; then
    sudo rm -rf data/store_* data/light_* data/publisher data/bootstrap_*
else
    rm -rf data/store_* data/light_* data/publisher data/bootstrap_* 2>/dev/null || \
    docker run --rm -v "$(pwd):/work" -w /work alpine rm -rf data/store_* data/light_* data/publisher data/bootstrap_* 2>/dev/null || true
fi
rm -f publisher.log light*.log bootstrap*.log store*.log data/completion.log
rm -f store_*.key *.key
rm -rf bin/

echo "[+] Cleanup completed successfully."
