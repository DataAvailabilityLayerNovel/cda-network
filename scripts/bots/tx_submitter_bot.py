#!/usr/bin/env python3
"""
tx_submitter_bot.py - CometBFT Transaction Submitter Bot for CDA Network.

Replaces the legacy block_publisher_bot.py:
1. Submits batches of transactions directly to CometBFT Consensus RPC (/broadcast_tx_sync).
2. Does NOT hardcode block names. Queries CometBFT to discover the exact block height where txns land.
3. Queries Publisher Node to inspect dynamic CDA parameters (K, ODS dimension, EDS dimension).
4. Waits for CDA Network BlockReady signal (Publisher -> Bootstrap -> Store Nodes).
5. Prints structured, real-time metrics per block.
"""

import argparse
import base64
import concurrent.futures
import json
import os
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def http_get_json(url, timeout=10):
    req = urllib.request.Request(url, headers={'Accept': 'application/json'})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode('utf-8'))


def submit_tx_to_cometbft(cometbft_rpc, tx_bytes, timeout=30):
    if isinstance(tx_bytes, str):
        tx_bytes = tx_bytes.encode('utf-8')
    b64_tx = base64.b64encode(tx_bytes).decode('ascii')
    payload = {
        "jsonrpc": "2.0",
        "id": int(time.time() * 1000) % 1000000,
        "method": "broadcast_tx_sync",
        "params": {
            "tx": b64_tx
        }
    }
    req_data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        f"{cometbft_rpc}",
        data=req_data,
        headers={'Content-Type': 'application/json'}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            res = json.loads(resp.read().decode('utf-8'))
            if "error" in res:
                raise RuntimeError(f"CometBFT RPC Error: {res['error']}")
            return res
    except urllib.error.HTTPError as e:
        err_body = e.read().decode('utf-8', errors='ignore').strip()
        raise RuntimeError(f"CometBFT HTTP {e.code}: {err_body if err_body else e.reason}")


def broadcast_tx_batch(cometbft_rpc, txs, max_workers=32, timeout=30):
    last_tx_hash = None
    if len(txs) == 1:
        res = submit_tx_to_cometbft(cometbft_rpc, txs[0], timeout=timeout)
        tx_res = res.get("result", {})
        code = tx_res.get("code", 0)
        if code != 0:
            raise RuntimeError(f"CheckTx rejected tx with code {code}: {tx_res.get('log')}")
        return tx_res.get("hash", "")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_tx = {
            executor.submit(submit_tx_to_cometbft, cometbft_rpc, tx, timeout=timeout): tx
            for tx in txs
        }
        for future in concurrent.futures.as_completed(future_to_tx):
            res = future.result()
            tx_res = res.get("result", {})
            code = tx_res.get("code", 0)
            if code != 0:
                raise RuntimeError(f"CheckTx rejected tx with code {code}: {tx_res.get('log')}")
            last_tx_hash = tx_res.get("hash", "")
    return last_tx_hash


def wait_for_tx_commit(cometbft_rpc, tx_hash, timeout=60, poll_interval=0.2):
    url = f"{cometbft_rpc}/tx?hash=0x{tx_hash}&prove=false"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as resp:
                data = json.loads(resp.read().decode('utf-8'))
                res = data.get("result", {})
                h = res.get("height")
                if h:
                    return int(h)
        except Exception:
            pass
        time.sleep(poll_interval)
    raise TimeoutError(f"Tx {tx_hash} not committed within {timeout}s")


def estimate_k(total_cells, min_k=8, max_k=64):
    k = min_k
    while k < max_k and k * k < total_cells:
        k *= 2
    return k


def count_cells_for_txs(txs, cell_size=512):
    count = 0
    for tx in txs:
        if len(tx) == 2 * cell_size or len(tx) == 128:
            try:
                bytes.fromhex(tx.decode('ascii'))
                count += 1
                continue
            except Exception:
                pass
        chunks = (len(tx) + cell_size - 1) // cell_size
        count += max(1, chunks)
    return count


def generate_transactions(count, payload_size=512, tx_format="kv", cell_size=512):
    txs = []
    batch_prefix = uuid.uuid4().hex[:8]

    if tx_format == "raw_hex":
        for _ in range(count):
            hex_data = os.urandom(payload_size).hex()
            txs.append(hex_data.encode('ascii'))
        return txs

    # Default 'kv' format: tx_<prefix>_<i>=<payload_hex>
    for i in range(count):
        key = f"tx_{batch_prefix}_{i}"
        payload = os.urandom(payload_size)
        tx_str = f"{key}={payload.hex()}"
        txs.append(tx_str.encode('utf-8'))
    return txs


def wait_for_block_ready(bootstrap_url, expected_block_id, timeout=120, poll_interval=0.2):
    latest_url = f"{bootstrap_url}/block-ready/latest"
    start = time.time()
    deadline = start + timeout
    try:
        exp_h = int(expected_block_id.split("-")[-1])
    except (ValueError, IndexError):
        exp_h = 0

    while time.time() < deadline:
        try:
            with urllib.request.urlopen(latest_url, timeout=3) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode('utf-8'))
                    b_id = data.get("block_id")
                    h = data.get("height", 0)
                    k = data.get("k", 0)
                    if b_id == expected_block_id or (exp_h > 0 and h >= exp_h):
                        return True, time.time() - start, k
        except Exception:
            pass
        time.sleep(poll_interval)
    return False, time.time() - start, 0


def fetch_publisher_block_info(publisher_url, block_id, max_retries=20):
    url = f"{publisher_url}/header/{block_id}"
    for _ in range(max_retries):
        try:
            data = http_get_json(url, timeout=3)
            if data and "column_comm" in data:
                n = len(data["column_comm"])
                k = n // 2
                return {
                    "k": k,
                    "n": n,
                    "ods_cells": k * k,
                    "eds_cells": n * n,
                    "commits_root": data.get("commits_root", "")[:16] + "...",
                    "chain_id": data.get("chain_id", ""),
                }
        except Exception:
            time.sleep(0.15)
    return None


def main():
    parser = argparse.ArgumentParser(description="CDA Network - CometBFT Transaction Submitter Bot")
    parser.add_argument("--cometbft", type=str, default="http://localhost:26657", help="CometBFT RPC endpoint")
    parser.add_argument("--publisher", type=str, default="http://localhost:8080", help="CDA Publisher API endpoint")
    parser.add_argument("--bootstrap", type=str, default="http://localhost:9200", help="CDA Bootstrap API endpoint")
    parser.add_argument("--tx-count", type=int, default=64, help="Number of transactions per batch (default: 64)")
    parser.add_argument("--payload-size", type=int, default=512, help="Payload data bytes per transaction (default: 512)")
    parser.add_argument("--cell-size", type=int, default=512, help="Cell size in bytes (default: 512)")
    parser.add_argument("--tx-format", type=str, default="kv", choices=["kv", "raw_hex"], help="Tx format: 'kv' (key=value) or 'raw_hex' (512B cell)")
    parser.add_argument("--interval", type=float, default=2.0, help="Interval between batches in seconds (default: 2.0)")
    parser.add_argument("--batches", type=int, default=0, help="Number of batches to submit (0 = continuous, default: 0)")
    parser.add_argument("--workers", type=int, default=32, help="Concurrency workers for broadcasting (default: 32)")
    parser.add_argument("--wait-cda", action="store_true", default=True, help="Wait for CDA BlockReady before next batch")
    args = parser.parse_args()

    print("=" * 80)
    print("CDA NETWORK - COMETBFT TRANSACTION SUBMITTER BOT")
    print(f"CometBFT RPC    : {args.cometbft}")
    print(f"CDA Publisher   : {args.publisher}")
    print(f"CDA Bootstrap   : {args.bootstrap}")
    print(f"Tx Count / Batch: {args.tx_count}")
    print(f"Payload / Tx    : {args.payload_size} bytes ({args.tx_format} format)")
    print(f"Cell Size       : {args.cell_size} bytes")
    print(f"Batch Interval  : {args.interval}s")
    print(f"Total Batches   : {'Continuous (Infinite)' if args.batches == 0 else args.batches}")
    print("=" * 80)

    # Health check
    try:
        status = http_get_json(f"{args.cometbft}/status", timeout=5)
        sync_info = status.get("result", {}).get("sync_info", {})
        node_info = status.get("result", {}).get("node_info", {})
        print(f"✅ CometBFT connected | Node: {node_info.get('moniker')} | Height: {sync_info.get('latest_block_height')}")
    except Exception as e:
        print(f"❌ Cannot connect to CometBFT at {args.cometbft}: {e}")
        sys.exit(1)

    batch_idx = 0
    total_submitted = 0
    start_time = time.time()

    try:
        while True:
            batch_idx += 1
            if args.batches > 0 and batch_idx > args.batches:
                break

            t_batch_start = time.time()
            txs = generate_transactions(args.tx_count, payload_size=args.payload_size, tx_format=args.tx_format, cell_size=args.cell_size)
            total_bytes = sum(len(tx) for tx in txs)
            cell_count = count_cells_for_txs(txs, cell_size=args.cell_size)
            expected_k = estimate_k(cell_count)
            print(f"\n[Batch #{batch_idx:03d}] Submitting {len(txs)} txs | Total Data: {total_bytes / 1024:.2f} KB | Cells: {cell_count} | Expected K: {expected_k} ({expected_k}x{expected_k}={expected_k*expected_k} cells)...")

            # 1. Broadcast txs
            t_broadcast = time.time()
            try:
                tx_hash = broadcast_tx_batch(args.cometbft, txs, max_workers=args.workers, timeout=30)
                dur_broadcast = time.time() - t_broadcast
                total_submitted += len(txs)
            except Exception as e:
                print(f"  ❌ Broadcast failed: {e}")
                time.sleep(args.interval)
                continue

            # 2. Query CometBFT to discover block height
            t_commit = time.time()
            try:
                height = wait_for_tx_commit(args.cometbft, tx_hash, timeout=60)
                dur_consensus = time.time() - t_commit
                block_id = f"block-{height}"
                print(f"  ✅ CometBFT committed into Height #{height} ({block_id}) | Consensus Latency: {dur_consensus:.3f}s")
            except Exception as e:
                print(f"  ❌ Tx commit timeout: {e}")
                time.sleep(args.interval)
                continue

            # 3. Query Publisher for CDA dynamic parameters
            info = fetch_publisher_block_info(args.publisher, block_id)
            k_str = f"K={info['k']} (ODS: {info['k']}x{info['k']}, EDS: {info['n']}x{info['n']})" if info else "K=Dynamic"

            # 4. Wait for CDA Network BlockReady
            if args.wait_cda:
                ready, dur_cda, ready_k = wait_for_block_ready(args.bootstrap, block_id, timeout=120)
                total_dur = time.time() - t_batch_start
                if ready:
                    k_final = ready_k if ready_k > 0 else (info['k'] if info else 'Dynamic')
                    print(f"  🎉 [BLOCK_COMPLETED] {block_id} reached CDA BlockReady in {dur_cda:.3f}s | K={k_final} | Total Batch Time: {total_dur:.3f}s")
                else:
                    print(f"  ⚠️ Timeout waiting for CDA BlockReady for {block_id} after {dur_cda:.3f}s")

            # 5. Sleep interval before next batch
            if args.interval > 0:
                time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nBot stopped by user.")

    elapsed = time.time() - start_time
    print("\n" + "=" * 80)
    print(f"Summary: {batch_idx - 1} batches ({total_submitted} total transactions) submitted in {elapsed:.1f}s")
    print("=" * 80)


if __name__ == '__main__':
    main()
