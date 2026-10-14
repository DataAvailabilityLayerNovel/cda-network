#!/usr/bin/env python3
"""
init_cometbft_cluster.py - Initialize CometBFT multi-validator cluster configurations for Docker Compose.

This script runs `cometbft testnet` with hostname-prefix matching Docker service names,
and adjusts config.toml for containerized networking.
"""

import argparse
import os
import re
import shutil
import subprocess
import sys


def ensure_cometbft_bin(bin_path="./bin/cometbft"):
    if os.path.isfile(bin_path) and os.access(bin_path, os.X_OK):
        return os.path.abspath(bin_path)

    system_bin = shutil.which("cometbft")
    if system_bin:
        return system_bin

    print(f"[init_cometbft_cluster] Building cometbft binary at {bin_path}...")
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cmd = ["go", "build", "-ldflags=-s -w", "-o", bin_path, "./cometbft/cmd/cometbft"]
    res = subprocess.run(cmd, cwd=root_dir)
    if res.returncode != 0:
        raise RuntimeError("Failed to build cometbft binary")
    return os.path.abspath(bin_path)


def init_cometbft_cluster(num_validators=4, output_dir="./data/cometbft",
                          timeout_commit="1s", clean=True, bin_path="./bin/cometbft"):
    output_dir = os.path.abspath(output_dir)
    cometbft_bin = ensure_cometbft_bin(bin_path)

    if clean and os.path.exists(output_dir):
        print(f"[init_cometbft_cluster] Cleaning old cluster configs at {output_dir}...")
        try:
            shutil.rmtree(output_dir)
        except Exception as e:
            res_sudo = subprocess.run(["sudo", "-n", "rm", "-rf", output_dir], capture_output=True)
            if res_sudo.returncode != 0:
                print(f"[init_cometbft_cluster] Notice: Could not remove {output_dir} directly ({e}). Skipping cleanup.")

    try:
        os.makedirs(output_dir, exist_ok=True)
    except Exception:
        pass

    print(f"[init_cometbft_cluster] Initializing {num_validators} CometBFT validators in {output_dir}...")
    cmd = [
        cometbft_bin, "testnet",
        "--v", str(num_validators),
        "--hostname-prefix", "cometbft-",
        "--node-dir-prefix", "node",
        "--populate-persistent-peers",
        "--o", output_dir
    ]
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"cometbft testnet failed:\nSTDOUT: {res.stdout}\nSTDERR: {res.stderr}")

    print(res.stdout.strip())

    # Patch config.toml for each validator node
    for i in range(num_validators):
        node_dir = os.path.join(output_dir, f"node{i}")
        config_path = os.path.join(node_dir, "config", "config.toml")
        if not os.path.isfile(config_path):
            print(f"[Warning] config.toml not found at {config_path}")
            continue

        with open(config_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Bind RPC to 0.0.0.0
        content = re.sub(
            r'laddr\s*=\s*"tcp://127\.0\.0\.1:26657"',
            'laddr = "tcp://0.0.0.0:26657"',
            content
        )

        # Allow all CORS origins
        content = re.sub(
            r'cors_allowed_origins\s*=\s*\[\]',
            'cors_allowed_origins = ["*"]',
            content
        )

        # Set block commit timeout
        if timeout_commit:
            content = re.sub(
                r'timeout_commit\s*=\s*"[^"]*"',
                f'timeout_commit = "{timeout_commit}"',
                content
            )

        # Enable Prometheus metrics
        content = re.sub(
            r'prometheus\s*=\s*false',
            'prometheus = true',
            content
        )

        # Disable empty blocks so blocks strictly correspond to transaction batches
        content = re.sub(
            r'create_empty_blocks\s*=\s*true',
            'create_empty_blocks = false',
            content
        )

        with open(config_path, "w", encoding="utf-8") as f:
            f.write(content)

    print(f"[init_cometbft_cluster] Successfully configured {num_validators} validator nodes.")
    return output_dir


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Initialize CometBFT Cluster Configuration")
    parser.add_argument("-v", "--validators", type=int, default=4, help="Number of validator nodes (default: 4)")
    parser.add_argument("-o", "--output-dir", type=str, default="./data/cometbft", help="Output directory")
    parser.add_argument("--timeout-commit", type=str, default="1s", help="timeout_commit duration (default: 1s)")
    parser.add_argument("--bin", dest="bin_path", type=str, default="./bin/cometbft", help="Path to cometbft binary")
    parser.add_argument("--no-clean", dest="clean", action="store_false", help="Do not delete existing output directory")

    args = parser.parse_args()
    init_cometbft_cluster(
        num_validators=args.validators,
        output_dir=args.output_dir,
        timeout_commit=args.timeout_commit,
        clean=args.clean,
        bin_path=args.bin_path
    )
