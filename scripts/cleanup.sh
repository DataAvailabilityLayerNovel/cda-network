#!/bin/bash
# cleanup.sh - Stop containers and delete host data files

# 1. Stop native background test processes and test runners (if running)
echo "[*] Terminating native CDA node processes and test runners..."
# Kill any background benchmark and test runner scripts
pkill -9 -f "scripts/tests/.*(\.py|\.sh)" 2>/dev/null || true
pkill -9 -f "benchmark_matrix_runner|run_cda_isolated_matrix|test_pipeline_block_timing" 2>/dev/null || true

# Kill native node binaries regardless of path (bin/node, ./node, go run, or cmd path)
pkill -9 -f "(^|/|\s)(publisher|bootstrap|store|light|cometbft)(\s|$)" 2>/dev/null || true
pkill -9 -f "cda-(publisher|bootstrap|store|light)-node" 2>/dev/null || true
pkill -9 -f "go test.*(cda|cometbft)" 2>/dev/null || true

# 2. Stop docker compose containers and clean up docker networks/proxies
DOCKER_BIN=""
if command -v docker >/dev/null 2>&1; then
    if docker info >/dev/null 2>&1; then
        DOCKER_BIN="docker"
    elif sudo -n docker info >/dev/null 2>&1; then
        DOCKER_BIN="sudo docker"
    fi
fi

if [ -n "$DOCKER_BIN" ]; then
    echo "[*] Stopping docker compose containers and removing volumes..."
    
    if [ -f "docker-compose-benchmark.yml" ]; then
        $DOCKER_BIN compose -f docker-compose-benchmark.yml down -v -t 1 --remove-orphans 2>/dev/null || true
    fi
    
    if [ -f "docker-compose.json" ]; then
        $DOCKER_BIN compose -f docker-compose.json down -v -t 1 --remove-orphans 2>/dev/null || true
    fi

    if [ -f "docker-compose.yml" ]; then
        $DOCKER_BIN compose -f docker-compose.yml down -v -t 1 --remove-orphans 2>/dev/null || true
    fi

    # Force remove any remaining containers related to cda-network or cda services
    CDA_CONTAINERS=$($DOCKER_BIN ps -aq --filter "name=cda-network" 2>/dev/null || true)
    if [ -n "$CDA_CONTAINERS" ]; then
        echo "[*] Force removing remaining CDA containers..."
        echo "$CDA_CONTAINERS" | xargs -r $DOCKER_BIN rm -f >/dev/null 2>&1 || true
    fi

    # Also clean up containers attached to cda-net if any remain
    NET_CONTAINERS=$($DOCKER_BIN ps -aq --filter "network=cda-network_cda-net" 2>/dev/null || true)
    if [ -n "$NET_CONTAINERS" ]; then
        echo "$NET_CONTAINERS" | xargs -r $DOCKER_BIN rm -f >/dev/null 2>&1 || true
    fi
fi

# 3. Kill any lingering processes holding CDA network ports (HTTP, Libp2p, CometBFT, Monitoring)
echo "[*] Ensuring all CDA ports are released..."
PORTS_TO_FREE=(8080 8888 9090 3000 26656 26657 18080 18888)
# Add bootstrap and store HTTP & P2P port ranges if fuser is available
if command -v fuser >/dev/null 2>&1; then
    for p in "${PORTS_TO_FREE[@]}"; do
        fuser -k -9 "${p}/tcp" 2>/dev/null || true
        fuser -k -9 "${p}/udp" 2>/dev/null || true
    done
    # Bootstraps & stores: 8100-8200, 9200-9400, 18086-18100, 19200-19400
    for ((p=8086; p<=8110; p++)); do fuser -k -9 "${p}/tcp" 2>/dev/null || true; done
    for ((p=9200; p<=9210; p++)); do fuser -k -9 "${p}/tcp" 2>/dev/null || true; done
fi

# Fallback: find PIDs via lsof or ss if fuser was not present
if command -v lsof >/dev/null 2>&1; then
    LSOF_PIDS=$(lsof -ti:8080,8888,9090,3000,26656,26657,18080,18888 2>/dev/null || true)
    if [ -n "$LSOF_PIDS" ]; then
        echo "$LSOF_PIDS" | xargs -r kill -9 2>/dev/null || true
    fi
fi

# 4. Clean up local databases, logs, identity keys, and build artifacts
echo "[*] Cleaning up local data directories, databases, logs, and store keys..."
if sudo -n true 2>/dev/null; then
    sudo rm -rf data/store_* data/light_* data/publisher data/bootstrap_* data/cometbft
else
    rm -rf data/store_* data/light_* data/publisher data/bootstrap_* data/cometbft 2>/dev/null || \
    docker run --rm -v "$(pwd):/work" -w /work alpine rm -rf data/store_* data/light_* data/publisher data/bootstrap_* data/cometbft 2>/dev/null || true
fi
rm -f publisher.log light*.log bootstrap*.log store*.log data/completion.log
rm -f publisher_tx_test.log cometbft*.log
rm -f store_*.key *.key

if [ -f "scripts/init_cometbft_cluster.py" ]; then
    echo "[*] Re-initializing CometBFT cluster configurations with shared genesis..."
    python3 scripts/init_cometbft_cluster.py -v 4 2>/dev/null || true
fi

echo "[+] Cleanup completed successfully."
