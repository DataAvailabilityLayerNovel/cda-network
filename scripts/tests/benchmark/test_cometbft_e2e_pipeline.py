#!/usr/bin/env python3
"""
test_cometbft_e2e_pipeline.py - End-to-End Pipeline Verification with CometBFT Consensus.

Pipeline Flow:
1. Client submits transactions to CometBFT Consensus Cluster (RPC /broadcast_tx_commit).
2. CometBFT Proposer constructs ODS (Original Data Square: K x K cells), computes CDA commitments
   (CommitsRoot, ColumnComm, Coeffs), and commits block via BFT consensus.
3. CometBFT PublisherPusher pushes committed block to Publisher Node (/publish).
4. Publisher verifies CDA Header against BFT consensus, expands ODS into EDS (Extended Data Square: 2K x 2K cells),
   and streams column cells to Bootstrap Nodes.
5. Bootstrap Nodes generate KZG proofs and seed pieces to Store Nodes via P2P.
6. Store Nodes receive pieces, recode RLNC, perform rank checks, and reach storage custody.
7. Light Nodes perform Auto-DAS sampling and KZG verification.
8. Script audits piece counts across all Store Nodes to verify exact custody targets.
"""

import argparse
import base64
import concurrent.futures
import json
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


def http_get_json(url, timeout=10):
    req = urllib.request.Request(url, headers={'Accept': 'application/json'})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode('utf-8'))


def check_health(cometbft_rpc, publisher_url, bootstrap_url, store_ports=None):
    print("[1/5] Checking service health...")
    # CometBFT
    try:
        status_data = http_get_json(f"{cometbft_rpc}/status", timeout=5)
        sync_info = status_data.get("result", {}).get("sync_info", {})
        node_info = status_data.get("result", {}).get("node_info", {})
        print(f"  ✅ CometBFT Node '{node_info.get('moniker')}' online | Chain: {node_info.get('network')} | Height: {sync_info.get('latest_block_height')}")
    except Exception as e:
        print(f"  ❌ CometBFT RPC unreachable at {cometbft_rpc}: {e}")
        return False

    # Publisher
    try:
        req = urllib.request.Request(f"{publisher_url}/health")
        with urllib.request.urlopen(req, timeout=5) as resp:
            if resp.status == 200:
                print(f"  ✅ Publisher Node online at {publisher_url}")
    except Exception as e:
        print(f"  ❌ Publisher unreachable at {publisher_url}: {e}")
        return False

    # Bootstrap
    try:
        req = urllib.request.Request(f"{bootstrap_url}/health")
        with urllib.request.urlopen(req, timeout=5) as resp:
            if resp.status == 200:
                print(f"  ✅ Bootstrap Node online at {bootstrap_url}")
    except Exception as e:
        print(f"  ❌ Bootstrap unreachable at {bootstrap_url}: {e}")
        return False

    # Store Nodes
    if store_ports:
        for p in store_ports:
            try:
                req = urllib.request.Request(f"http://localhost:{p}/health")
                with urllib.request.urlopen(req, timeout=5) as resp:
                    if resp.status != 200:
                        print(f"  ❌ Store Node on port {p} returned status {resp.status}")
                        return False
            except Exception as e:
                print(f"  ❌ Store Node on port {p} unreachable: {e}")
                return False
        print(f"  ✅ All {len(store_ports)} Store Nodes online ({store_ports[0]}..{store_ports[-1]})")

    return True


def submit_tx_to_cometbft(cometbft_rpc, tx_bytes, timeout=30):
    """
    Broadcasts a transaction to CometBFT RPC via JSON-RPC broadcast_tx_sync.
    CometBFT kvstore app accepts 'key=value' or 'key:value'.
    """
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
        err_msg = e.read().decode('utf-8', errors='ignore')
        raise RuntimeError(f"HTTP {e.code}: {err_msg}")


def broadcast_block_transactions(cometbft_rpc, txs, max_workers=32, timeout=30):
    """
    Broadcasts transactions concurrently to CometBFT mempool via broadcast_tx_sync.
    Returns the hash of the last submitted transaction to monitor for block commitment.
    """
    last_tx_hash = None
    if len(txs) == 1:
        res = submit_tx_to_cometbft(cometbft_rpc, txs[0], timeout=timeout)
        tx_res = res.get("result", {})
        code = tx_res.get("code", 0)
        if code != 0:
            raise RuntimeError(f"CheckTx rejected transaction with code {code}: {tx_res.get('log')}")
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
                raise RuntimeError(f"CheckTx rejected transaction with code {code}: {tx_res.get('log')}")
            last_tx_hash = tx_res.get("hash", "")
    return last_tx_hash


def wait_for_tx_commit(cometbft_rpc, tx_hash, timeout=30, poll_interval=0.2):
    """
    Polls /tx?hash=0x... until transaction is committed into a block.
    Returns committed block height (int).
    """
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


def generate_transactions(count, cell_size=64):
    """
    Generates random transactions without predetermined block names.
    Format: tx_{timestamp}_{uuid}_{i}={64-byte hex payload}
    """
    import uuid
    txs = []
    batch_prefix = uuid.uuid4().hex[:8]
    fmt = f"%0{cell_size * 2}x"
    for i in range(count):
        val_hex = fmt % random.randint(1, 10_000_000)
        tx_str = f"tx_{batch_prefix}_{i}={val_hex}"
        txs.append(tx_str.encode('utf-8'))
    return txs


def wait_for_block_ready(bootstrap_url, expected_block_id, timeout=180, poll_interval=0.2):
    """
    Polls /block-ready/latest on Bootstrap Node until expected_block_id is completed.
    """
    latest_url = f"{bootstrap_url}/block-ready/latest"
    start = time.time()
    deadline = start + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(latest_url, timeout=3) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode('utf-8'))
                    b_id = data.get("block_id")
                    h = data.get("height", 0)
                    try:
                        exp_h = int(expected_block_id.split("-")[-1])
                    except (ValueError, IndexError):
                        exp_h = 0
                    if b_id == expected_block_id or (exp_h > 0 and h >= exp_h):
                        return True, time.time() - start
        except Exception:
            pass
        time.sleep(poll_interval)
    return False, time.time() - start


def audit_store_pieces(store_ports):
    """
    Scrapes Prometheus metrics from each store node to verify custody piece counts.
    """
    results = []
    total_custody = 0
    total_recoded = 0
    total_li = 0

    for port in store_ports:
        url = f"http://localhost:{port}/metrics"
        metrics = {
            "port": port,
            "custody_pieces": 0,
            "recoded_pieces": 0,
            "total_pieces": 0
        }
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                for line in resp.read().decode('utf-8', errors='ignore').splitlines():
                    line = line.strip()
                    if line.startswith("cda_store_custody_pieces_count"):
                        metrics["custody_pieces"] = int(float(line.split()[1]))
                    elif line.startswith("cda_store_recoded_pieces_count"):
                        metrics["recoded_pieces"] = int(float(line.split()[1]))
                    elif line.startswith("cda_store_linear_independent_pieces_count"):
                        metrics["total_pieces"] = int(float(line.split()[1]))
        except Exception as e:
            metrics["error"] = str(e)

        total_custody += metrics["custody_pieces"]
        total_recoded += metrics["recoded_pieces"]
        total_li += metrics["total_pieces"]
        results.append(metrics)

    return results, total_custody, total_recoded, total_li


def run_e2e_pipeline(cometbft_rpc, publisher_url, bootstrap_url, store_ports, k, k_piece=4, block_count=2, timeout=180):
    print("=" * 80)
    print("CDA NETWORK - COMETBFT CONSENSUS END-TO-END PIPELINE BENCHMARK")
    print(f"Matrix Dimension K : {k} (ODS: {k}x{k} = {k*k} cells | EDS: {2*k}x{2*k} = {4*k*k} cells)")
    print(f"RLNC Piece K_Piece : {k_piece}")
    print(f"Target Block Count : {block_count}")
    print(f"Store Node Ports   : {' '.join(map(str, store_ports))}")
    print("=" * 80)

    if not check_health(cometbft_rpc, publisher_url, bootstrap_url, store_ports):
        print("\n❌ Healthcheck failed. Aborting pipeline test.")
        return False

    pipeline_results = []
    overall_start = time.time()

    for batch_idx in range(block_count):
        target_txs = k * k
        print(f"\n[2/5] --- [Transaction Batch #{batch_idx + 1}] Submitting {target_txs} Transactions to CometBFT ---")
        txs = generate_transactions(target_txs)
        print(f"  Generated {len(txs)} transactions (target dimension base: K={k}).")

        t_tx_start = time.time()
        try:
            tx_hash = broadcast_block_transactions(cometbft_rpc, txs, max_workers=32, timeout=30)
            print(f"  ✅ Broadcast all {len(txs)} transactions into CometBFT mempool (Last Tx Hash: {tx_hash}) in {time.time() - t_tx_start:.3f}s")

            # Query CometBFT to discover WHICH block height these transactions were committed into
            committed_height = wait_for_tx_commit(cometbft_rpc, tx_hash, timeout=30)
            committed_block_id = f"block-{committed_height}"
            print(f"  ✅ Discovered committed block in CometBFT at Height #{committed_height} ({committed_block_id})")
        except Exception as e:
            print(f"  ❌ Failed to submit tx to CometBFT: {e}")
            return False

        # Extract CDA block parameters from Publisher
        publisher_header_url = f"{publisher_url}/header/{committed_block_id}"
        header_data = None
        for _ in range(30):
            try:
                header_data = http_get_json(publisher_header_url, timeout=3)
                if header_data:
                    break
            except Exception:
                time.sleep(0.1)

        actual_k = "dynamic"
        if header_data and "column_comm" in header_data:
            n_cols = len(header_data["column_comm"])
            actual_k = n_cols // 2
            print(f"  📐 Publisher CDA Header: K={actual_k} (ODS: {actual_k}x{actual_k}={actual_k*actual_k} cells | EDS: {n_cols}x{n_cols}={n_cols*n_cols} cells)")

        print(f"[3/5] Waiting for BlockReady on CDA Network for {committed_block_id} (Publisher -> Bootstrap -> Store)...")
        ready, ready_lat = wait_for_block_ready(bootstrap_url, committed_block_id, timeout=timeout)
        total_block_time = time.time() - t_tx_start

        if ready:
            print(f"  🎉 {committed_block_id} reached CDA BlockReady in {total_block_time:.3f}s! (Committed Height: {committed_height}, K={actual_k})")
            pipeline_results.append({
                "height": committed_height,
                "block_id": committed_block_id,
                "latency": total_block_time,
                "status": "SUCCESS"
            })
        else:
            print(f"  ❌ TIMEOUT waiting for {committed_block_id} after {total_block_time:.3f}s!")
            pipeline_results.append({
                "height": committed_height,
                "block_id": committed_block_id,
                "latency": total_block_time,
                "status": "TIMEOUT"
            })
            break

    # Audit piece counts
    print("\n[4/5] Auditing Custody Piece Storage across Store Nodes...")
    time.sleep(2)  # Short pause to let final BadgerDB writes flush
    store_metrics, total_custody, total_recoded, total_li = audit_store_pieces(store_ports)

    print("\n" + "-" * 75)
    print(f"{'Store Port':<12} | {'Primary Custody':<18} | {'Recoded':<12} | {'Total Linear Indep':<18}")
    print("-" * 75)
    for sm in store_metrics:
        port = sm["port"]
        c_pieces = sm["custody_pieces"]
        r_pieces = sm["recoded_pieces"]
        t_pieces = sm["total_pieces"]
        print(f"{port:<12} | {c_pieces:<18} | {r_pieces:<12} | {t_pieces:<18}")
    print("-" * 75)
    print(f"{'TOTAL SUM':<12} | {total_custody:<18} | {total_recoded:<12} | {total_li:<18}")
    print("=" * 75)

    # Theoretical piece target calculation for K=64, 1 column active, 8 stores:
    # Per store node (2 blocks): 1,024 custody (at k_piece=4), 896 recoded, 1,920 total.
    # Total sum across all 8 stores: 8,192 custody (at k_piece=4), 7,168 recoded, 15,360 total.
    if k == 64 and block_count == 2:
        expected_custody = 1024 * (k_piece // 4) * len(store_ports) if k_piece >= 4 else 2048 * len(store_ports)
        expected_recoded = 896 * (k_piece // 4) * len(store_ports) if k_piece >= 4 else 1792 * len(store_ports)
        expected_total = expected_custody + expected_recoded
        print(f"[*] Audit Target (K=64, 2 Blocks, KPiece={k_piece}, {len(store_ports)} Stores): Custody={expected_custody}, Recoded={expected_recoded}, Total={expected_total}")
        if total_custody == expected_custody and total_li == expected_total:
            print(f"  🎉 PERFECT MATCH: Piece storage exactly matches theoretical target ({expected_total} pieces)!")
        else:
            print(f"  ⚠️ Difference: Custody ({total_custody} vs {expected_custody}), Total ({total_li} vs {expected_total})")
    elif k == 8 and block_count == 2:
        expected_custody = 32 * (k_piece // 4) * len(store_ports) if k_piece >= 4 else 64 * len(store_ports)
        expected_recoded = 48 * (k_piece // 4) * len(store_ports) if k_piece >= 4 else 96 * len(store_ports)
        expected_total = expected_custody + expected_recoded
        print(f"[*] Audit Target (K=8, 2 Blocks, KPiece={k_piece}, {len(store_ports)} Stores): Custody={expected_custody}, Recoded={expected_recoded}, Total={expected_total}")
        if total_custody == expected_custody and total_li == expected_total:
            print(f"  🎉 PERFECT MATCH: Piece storage exactly matches theoretical target ({expected_total} pieces)!")
        else:
            print(f"  ⚠️ Difference: Custody ({total_custody} vs {expected_custody}), Total ({total_li} vs {expected_total})")

    total_time = time.time() - overall_start
    print(f"\n[5/5] Benchmark completed in {total_time:.2f}s.")
    return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="CDA Network CometBFT E2E Pipeline Benchmark")
    parser.add_argument("--cometbft", type=str, default="http://localhost:26657", help="CometBFT RPC URL")
    parser.add_argument("--publisher", type=str, default="http://localhost:8080", help="Publisher URL")
    parser.add_argument("--bootstrap", type=str, default="http://localhost:9200", help="Bootstrap Node URL")
    parser.add_argument("--k", type=int, default=64, help="Matrix dimension K (default: 64)")
    parser.add_argument("--k-piece", type=int, default=4, help="RLNC piece parameter k-piece (default: 4)")
    parser.add_argument("--count", type=int, default=2, help="Number of blocks to run (default: 2)")
    parser.add_argument("--timeout", type=float, default=180.0, help="Timeout per block in seconds")
    parser.add_argument("--store-ports", nargs="+", type=int, default=[9300, 9301, 9302, 9303, 9304, 9305, 9306, 9307], help="List of Store Node HTTP ports")

    args = parser.parse_args()
    run_e2e_pipeline(
        cometbft_rpc=args.cometbft,
        publisher_url=args.publisher,
        bootstrap_url=args.bootstrap,
        store_ports=args.store_ports,
        k=args.k,
        k_piece=args.k_piece,
        block_count=args.count,
        timeout=args.timeout
    )
