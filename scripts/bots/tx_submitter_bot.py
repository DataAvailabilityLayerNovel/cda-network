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
import queue
import random
import sys
import threading
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
            log_msg = str(tx_res.get('log', ''))
            if "mempool is full" in log_msg.lower():
                raise RuntimeError("MempoolIsFull")
            raise RuntimeError(f"CheckTx rejected tx with code {code}: {log_msg}")
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
                log_msg = str(tx_res.get('log', ''))
                if "mempool is full" in log_msg.lower():
                    raise RuntimeError("MempoolIsFull")
                raise RuntimeError(f"CheckTx rejected tx with code {code}: {log_msg}")
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


def wait_for_block_ready(bootstrap_url, expected_block_id, publisher_url=None, timeout=120, poll_interval=0.15):
    latest_url = f"{bootstrap_url}/block-ready/latest"
    start = time.time()
    deadline = start + timeout

    while time.time() < deadline:
        # 1. Exact match on Bootstrap /block-ready/latest
        try:
            with urllib.request.urlopen(latest_url, timeout=2) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode('utf-8'))
                    if data.get("block_id") == expected_block_id:
                        return True, time.time() - start, data.get("k", 0)
        except Exception:
            pass

        # 2. Cross-check with Publisher /block/{expected_block_id} for da_quorum_ready
        if publisher_url:
            try:
                b_url = f"{publisher_url}/block/{expected_block_id}"
                with urllib.request.urlopen(b_url, timeout=2) as resp:
                    if resp.status == 200:
                        binfo = json.loads(resp.read().decode('utf-8'))
                        if binfo.get("da_quorum_ready") is True:
                            return True, time.time() - start, binfo.get("k", 0)
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


def run_stream_mode(args):
    stop_event = threading.Event()
    last_block_time = time.time()
    blocks_committed = 0
    total_txs_in_blocks = 0
    cda_blocks_ready = 0

    def chain_monitor_worker():
        nonlocal last_block_time, blocks_committed, total_txs_in_blocks, cda_blocks_ready
        last_h = 0
        last_cda_block = ""
        while not stop_event.is_set():
            try:
                st = http_get_json(f"{args.cometbft}/status", timeout=2)
                cur_h = int(st.get("result", {}).get("sync_info", {}).get("latest_block_height", 0))
                if last_h == 0:
                    last_h = cur_h
                elif cur_h > last_h:
                    now = time.time()
                    dur = now - last_block_time
                    last_block_time = now
                    for h in range(last_h + 1, cur_h + 1):
                        blk = http_get_json(f"{args.cometbft}/block?height={h}", timeout=2)
                        tx_list = blk.get("result", {}).get("block", {}).get("data", {}).get("txs") or []
                        n_tx = len(tx_list)
                        blocks_committed += 1
                        total_txs_in_blocks += n_tx
                        print(f"  📦 [CometBFT] Block #{h} committed ({n_tx} txs) in {dur:.2f}s")
                    last_h = cur_h
            except Exception:
                pass

            try:
                cda = http_get_json(f"{args.bootstrap}/block-ready/latest", timeout=2)
                if cda and cda.get("block_id"):
                    b_id = cda.get("block_id")
                    if b_id != last_cda_block:
                        last_cda_block = b_id
                        h = cda.get("height", 0)
                        k = cda.get("k", 0)
                        cda_blocks_ready += 1
                        print(f"  🎉 [CDA Ready] {b_id} (Height #{h}) reached BlockReady | K={k}")
            except Exception:
                pass

            time.sleep(0.3)

    if args.watch:
        mon_thread = threading.Thread(target=chain_monitor_worker, daemon=True)
        mon_thread.start()

    batch_idx = 0
    total_submitted = 0
    start_time = time.time()
    print(f"\n🚀 Continuous Transaction Stream active. Submitting batches of {args.tx_count} txs...")
    print(f"   (Press Ctrl+C to stop streaming at any time)\n")

    try:
        while True:
            batch_idx += 1
            if args.batches > 0 and batch_idx > args.batches:
                break

            txs = generate_transactions(args.tx_count, payload_size=args.payload_size, tx_format=args.tx_format, cell_size=args.cell_size)
            total_bytes = sum(len(tx) for tx in txs)

            t_bcast = time.time()
            submitted = False
            while not submitted and not stop_event.is_set():
                try:
                    broadcast_tx_batch(args.cometbft, txs, max_workers=args.workers, timeout=30)
                    dur_bcast = time.time() - t_bcast
                    total_submitted += len(txs)
                    submitted = True
                except RuntimeError as e:
                    if "MempoolIsFull" in str(e):
                        print(f"  ⚠️ CometBFT Mempool is full (5000 limit), waiting 0.3s for block clearance...")
                        time.sleep(0.3)
                    else:
                        print(f"  ❌ Broadcast error: {e}")
                        time.sleep(0.5)
                        break
                except Exception as e:
                    print(f"  ❌ Broadcast error: {e}")
                    time.sleep(0.5)
                    break

            if not submitted:
                continue

            rate = len(txs) / max(0.001, dur_bcast)
            overall_rate = total_submitted / max(0.001, time.time() - start_time)
            print(f"[Batch #{batch_idx:04d}] Broadcast {len(txs)} txs ({total_bytes / 1024:.1f} KB) in {dur_bcast:.3f}s ({rate:.0f} tx/s) | Total: {total_submitted} txs (avg {overall_rate:.0f} tx/s)")

            if args.interval > 0:
                time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n\n🛑 Stream stopped by user.")
    finally:
        stop_event.set()

    elapsed = time.time() - start_time
    print("\n" + "=" * 80)
    print(f"STREAM SUMMARY: {batch_idx - 1} batches ({total_submitted} transactions) in {elapsed:.1f}s")
    print(f"Average Ingestion Rate : {total_submitted / max(0.001, elapsed):.1f} tx/s")
    if args.watch:
        print(f"Observed Activity      : {blocks_committed} blocks committed ({total_txs_in_blocks} txs included), {cda_blocks_ready} CDA blocks ready")
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(description="CDA Network - CometBFT Transaction Submitter Bot")
    parser.add_argument("--cometbft", type=str, default="http://localhost:26657", help="CometBFT RPC endpoint")
    parser.add_argument("--publisher", type=str, default="http://localhost:8080", help="CDA Publisher API endpoint")
    parser.add_argument("--bootstrap", type=str, default="http://localhost:9200", help="CDA Bootstrap API endpoint")
    parser.add_argument("--tx-count", type=int, default=64, help="Number of transactions per batch (default: 64)")
    parser.add_argument("--payload-size", type=int, default=512, help="Payload data bytes per transaction (default: 512)")
    parser.add_argument("--cell-size", type=int, default=512, help="Cell size in bytes (default: 512)")
    parser.add_argument("--tx-format", type=str, default="kv", choices=["kv", "raw_hex"], help="Tx format: 'kv' (key=value) or 'raw_hex' (512B cell)")
    parser.add_argument("--interval", type=float, default=2.0, help="Interval between batches in seconds (default: 2.0; set to 0.0 for continuous consecutive blocks)")
    parser.add_argument("--batches", type=int, default=0, help="Number of batches to submit (0 = continuous, default: 0)")
    parser.add_argument("--workers", type=int, default=32, help="Concurrency workers for broadcasting (default: 32)")
    parser.add_argument("--stream", "--continuous", dest="stream", action="store_true", default=False, help="Continuous streaming mode: injects transactions continuously into CometBFT without waiting for block commits")
    parser.add_argument("--no-wait-commit", dest="stream", action="store_true", help="Alias for --stream: do not wait for block commit before submitting next transactions")
    parser.add_argument("--watch", action=argparse.BooleanOptionalAction, default=True, help="Enable background monitoring of committed blocks and CDA readiness in stream mode")
    parser.add_argument("--wait-cda", action=argparse.BooleanOptionalAction, default=True, help="Wait for CDA BlockReady (use --no-wait-cda to skip entirely)")
    parser.add_argument("--pipeline", action="store_true", default=False, help="Enable pipelined continuous submission: submits txs consecutively without blocking on CDA BlockReady in the foreground")
    parser.add_argument("--max-in-flight", type=int, default=2, help="Maximum in-flight blocks in CDA pipeline before throttling in pipeline mode (default: 2, 0=unlimited)")
    args = parser.parse_args()

    mode_str = "CONTINUOUS STREAM (Non-blocking tx injection)" if args.stream else (
        f"PIPELINED (Max in-flight: {args.max_in_flight})" if args.pipeline else "SYNCHRONOUS (Wait commit & CDA)"
    )

    print("=" * 80)
    print("CDA NETWORK - COMETBFT TRANSACTION SUBMITTER BOT")
    print(f"CometBFT RPC    : {args.cometbft}")
    print(f"CDA Publisher   : {args.publisher}")
    print(f"CDA Bootstrap   : {args.bootstrap}")
    print(f"Submission Mode : {mode_str}")
    print(f"Tx Count / Batch: {args.tx_count}")
    print(f"Payload / Tx    : {args.payload_size} bytes ({args.tx_format} format)")
    print(f"Cell Size       : {args.cell_size} bytes")
    print(f"Batch Interval  : {args.interval}s")
    if not args.stream:
        print(f"Pipelined Mode  : {'ENABLED (Max in-flight: ' + str(args.max_in_flight) + ')' if args.pipeline else 'DISABLED (Synchronous foreground wait)'}")
        print(f"Wait CDA Ready  : {args.wait_cda}")
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

    if args.stream:
        run_stream_mode(args)
        return

    cda_queue = queue.Queue()
    in_flight_cond = threading.Condition()
    in_flight_count = 0
    cda_completed_count = 0
    cda_error_count = 0

    def cda_monitor_worker():
        nonlocal in_flight_count, cda_completed_count, cda_error_count
        while True:
            item = cda_queue.get()
            if item is None:
                cda_queue.task_done()
                break
            b_id, t_start, exp_k, b_idx, h = item
            ready, dur_cda, ready_k = wait_for_block_ready(args.bootstrap, b_id, publisher_url=args.publisher, timeout=120)
            tot_dur = time.time() - t_start
            if ready:
                cda_completed_count += 1
                k_fin = ready_k
                if k_fin <= 0:
                    info = fetch_publisher_block_info(args.publisher, b_id, max_retries=3)
                    k_fin = info['k'] if info else exp_k
                print(f"  🎉 [BLOCK_COMPLETED] {b_id} (Height #{h}, Batch #{b_idx}) reached CDA BlockReady in {dur_cda:.3f}s | K={k_fin} | Total Batch Time: {tot_dur:.3f}s")
            else:
                cda_error_count += 1
                print(f"  ⚠️ Timeout waiting for CDA BlockReady for {b_id} (Height #{h}, Batch #{b_idx}) after {dur_cda:.3f}s")

            with in_flight_cond:
                in_flight_count -= 1
                in_flight_cond.notify_all()
            cda_queue.task_done()

    if args.wait_cda and args.pipeline:
        monitor_thread = threading.Thread(target=cda_monitor_worker, daemon=True)
        monitor_thread.start()

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
                time.sleep(args.interval if args.interval > 0 else 0.5)
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
                time.sleep(args.interval if args.interval > 0 else 0.5)
                continue

            # 3. Wait for CDA Network BlockReady
            if args.wait_cda:
                if args.pipeline:
                    with in_flight_cond:
                        if args.max_in_flight > 0:
                            while in_flight_count >= args.max_in_flight:
                                in_flight_cond.wait(timeout=0.2)
                        in_flight_count += 1
                    cda_queue.put((block_id, t_batch_start, expected_k, batch_idx, height))
                else:
                    ready, dur_cda, ready_k = wait_for_block_ready(args.bootstrap, block_id, publisher_url=args.publisher, timeout=120)
                    total_dur = time.time() - t_batch_start
                    info = fetch_publisher_block_info(args.publisher, block_id, max_retries=5)
                    k_fin = ready_k if ready_k > 0 else (info['k'] if info else expected_k)
                    if ready:
                        print(f"  🎉 [BLOCK_COMPLETED] {block_id} reached CDA BlockReady in {dur_cda:.3f}s | K={k_fin} | Total Batch Time: {total_dur:.3f}s")
                    else:
                        print(f"  ⚠️ Timeout waiting for CDA BlockReady for {block_id} after {dur_cda:.3f}s")

            # 5. Sleep interval before next batch
            if args.interval > 0:
                time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nBot stopped by user.")

    if args.wait_cda and args.pipeline and not cda_queue.empty():
        print(f"\n[Pipelined Submission Finished] Waiting for remaining in-flight CDA blocks to complete...")
        cda_queue.join()

    elapsed = time.time() - start_time
    print("\n" + "=" * 80)
    print(f"Summary: {batch_idx - 1} batches ({total_submitted} total transactions) submitted in {elapsed:.1f}s")
    if args.wait_cda and args.pipeline:
        print(f"CDA Verification: {cda_completed_count} blocks completed, {cda_error_count} timeouts")
    print("=" * 80)


if __name__ == '__main__':
    main()
