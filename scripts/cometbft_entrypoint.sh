#!/usr/bin/env bash
set -e

# CometBFT Docker Entrypoint Script
# Supports both pre-generated testnet cluster volumes and standalone initialization.

HOME_DIR="${HOME_DIR:-/cometbft}"
NODE_ID="${NODE_ID:-0}"
PUBLISHER_URL="${PUBLISHER_URL:-http://publisher:8080/publish}"
CDA_K="${CDA_K:-64}"
CDA_K_PIECE="${CDA_K_PIECE:-4}"
PROXY_APP="${PROXY_APP:-kvstore}"

export HOME_DIR NODE_ID PUBLISHER_URL CDA_K CDA_K_PIECE PROXY_APP

echo "=========================================="
echo "Starting CometBFT Node (Node ID: ${NODE_ID})"
echo "Home Directory: ${HOME_DIR}"
echo "Publisher URL:  ${PUBLISHER_URL}"
echo "CDA K Matrix:   ${CDA_K} (ODS: ${CDA_K}x${CDA_K})"
echo "CDA K Piece:    ${CDA_K_PIECE}"
echo "=========================================="

# Check if configuration exists; if not, initialize standalone node
if [ ! -f "${HOME_DIR}/config/genesis.json" ]; then
    echo "[Entrypoint] No existing genesis.json found in ${HOME_DIR}. Initializing standalone node..."
    cometbft init --home "${HOME_DIR}"
fi

# Ensure RPC is bound to 0.0.0.0 and CORS is enabled for Docker network access
if [ -f "${HOME_DIR}/config/config.toml" ]; then
    sed -i 's|laddr = "tcp://127.0.0.1:26657"|laddr = "tcp://0.0.0.0:26657"|g' "${HOME_DIR}/config/config.toml"
    sed -i 's|cors_allowed_origins = \[\]|cors_allowed_origins = ["*"]|g' "${HOME_DIR}/config/config.toml"
fi

# If command begins with flags or "node", execute cometbft
if [ "$#" -eq 0 ]; then
    exec cometbft node --home "${HOME_DIR}" --proxy_app "${PROXY_APP}"
elif [ "$1" = "node" ]; then
    shift
    exec cometbft node --home "${HOME_DIR}" "$@"
elif [ "${1:0:1}" = "-" ]; then
    exec cometbft node --home "${HOME_DIR}" --proxy_app "${PROXY_APP}" "$@"
else
    exec cometbft "$@"
fi
