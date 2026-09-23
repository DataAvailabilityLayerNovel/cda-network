#!/usr/bin/env python3
"""
CDA Network - Strict Sequential Block Processing Timing Benchmark Script.

Publishes blocks strictly ONE AT A TIME. Wait for full BlockReady confirmation 
(which requires 100% completion of both Custody and Non-Custody cells)
before pushing the next block.

Measures exact processing latency per block and outputs performance metrics.
"""

import argparse
import json
import random
import sys
import time
import urllib.request
import urllib.error


def generate_ods_data(k, cell_size=64):
    """Generates k*k dummy cell field elements as hex strings (cell_size*2 hex chars)."""
    hex_len = cell_size * 2
    fmt = f'%0{hex_len}x'
    return [fmt % random.randint(1, 1_000_000) for _ in range(k * k)]


def publish_block(publish_url, block_id, k, timeout=60, cell_size=64):
    """Publish a single block. Returns (success, http_latency_seconds)."""
    data = generate_ods_data(k, cell_size=cell_size)
    payload = {"block_id": block_id, "data": data}
    req_data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        publish_url,
        data=req_data,
        headers={'Content-Type': 'application/json'},
    )
    start = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp.read()
        return True, time.time() - start
    except Exception as e:
        print(f"  [-] HTTP error publishing {block_id}: {e}")
        return False, time.time() - start


def wait_for_block_ready_sse(sse_url, expected_block_id, timeout=120):
    """
    Subscribe to SSE stream and block until expected_block_id appears.
    Returns (success, ready_latency_seconds).
    """
    start_time = time.time()
    deadline = start_time + timeout
    while time.time() < deadline:
        try:
            req = urllib.request.Request(sse_url, headers={'Accept': 'text/event-stream'})
            rem = max(1.0, deadline - time.time())
            with urllib.request.urlopen(req, timeout=rem) as resp:
                while time.time() < deadline:
                    line = resp.readline()
                    if not line:
                        break
                    line = line.strip()
                    if line.startswith(b"data:"):
                        raw = line[5:].strip()
                        try:
                            pl = json.loads(raw)
                            bid = pl.get("block_id", "")
                            if bid == expected_block_id:
                                return True, time.time() - start_time
                        except Exception:
                            pass
        except Exception:
            time.sleep(0.2)
    return False, time.time() - start_time


def wait_for_block_ready_poll(latest_url, expected_block_id, poll_interval=0.2, timeout=120):
    """
    Poll /block-ready/latest until expected_block_id appears.
    Returns (success, ready_latency_seconds).
    """
    start_time = time.time()
    deadline = start_time + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(latest_url, timeout=3) as resp:
                if resp.status == 200:
                    pl = json.loads(resp.read())
                    if pl.get("block_id") == expected_block_id:
                        return True, time.time() - start_time
        except Exception:
            pass
        time.sleep(poll_interval)
    return False, time.time() - start_time


def run_sequential_benchmark(publisher_url, bootstrap_url, k, count, timeout, cell_size=64):
    publish_api = f"{publisher_url.rstrip('/')}/publish"
    sse_api     = f"{bootstrap_url.rstrip('/')}/events/block-ready"
    latest_api  = f"{bootstrap_url.rstrip('/')}/block-ready/latest"

    print("=" * 70)
    print(" CDA NETWORK — STRICT SEQUENTIAL BLOCK PROCESSING BENCHMARK")
    print("=" * 70)
    print(f"[*] Publisher API : {publish_api}")
    print(f"[*] Bootstrap API : {bootstrap_url}")
    print(f"[*] Matrix K      : {k} (ODS cells: {k*k})")
    print(f"[*] Cell Size     : {cell_size} bytes (ODS Block: {(k*k*cell_size)/(1024*1024):.2f} MB)")
    print(f"[*] Target Blocks : {count}")
    print(f"[*] Max Timeout   : {timeout}s per block")
    print(f"[*] Mode          : Strictly Sequential (Block-by-Block Execution)")
    print("-" * 70)

    results = []
    total_benchmark_start = time.time()

    for height in range(1, count + 1):
        block_id = f"block-{height}"
        block_start = time.time()
        print(f"[{time.strftime('%H:%M:%S')}] Pushing {block_id} (height {height}/{count})...", end="", flush=True)

        ok, pub_lat = publish_block(publish_api, block_id, k, timeout=timeout, cell_size=cell_size)
        if not ok:
            print(f" FAILED to publish HTTP request.")
            continue

        print(f" Published ({pub_lat:.2f}s). Waiting for BlockReady (Custody + Non-Custody)...", end="", flush=True)

        # Wait for BlockReady signal (checking poll first then SSE)
        ready, ready_lat = wait_for_block_ready_poll(latest_api, block_id, poll_interval=0.1, timeout=0.3)
        if not ready:
            ready, ready_lat = wait_for_block_ready_sse(sse_api, block_id, timeout=timeout)
            if not ready:
                rem_timeout = max(1.0, timeout - (time.time() - block_start))
                ready, ready_lat = wait_for_block_ready_poll(latest_api, block_id, timeout=rem_timeout)

        total_block_time = time.time() - block_start

        if ready:
            print(f" SUCCESS! BlockReady reached in {total_block_time:.3f}s")
            results.append({
                "height": height,
                "block_id": block_id,
                "pub_latency": pub_lat,
                "total_time": total_block_time
            })
        else:
            print(f" TIMEOUT after {total_block_time:.3f}s!")

    total_benchmark_time = time.time() - total_benchmark_start

    print("\n" + "=" * 70)
    print(" BENCHMARK RESULTS SUMMARY")
    print("=" * 70)
    if not results:
        print("[-] No blocks successfully reached BlockReady status.")
        return

    avg_time = sum(r['total_time'] for r in results) / len(results)
    min_time = min(r['total_time'] for r in results)
    max_time = max(r['total_time'] for r in results)
    bpm = (len(results) / total_benchmark_time) * 60.0

    print(f"[*] Total Blocks Completed : {len(results)} / {count}")
    print(f"[*] Total Elapsed Time    : {total_benchmark_time:.2f} seconds")
    print(f"[*] Average Block Time    : {avg_time:.3f} seconds / block")
    print(f"[*] Min Block Time        : {min_time:.3f} seconds")
    print(f"[*] Max Block Time        : {max_time:.3f} seconds")
    print(f"[*] Processing Throughput : {bpm:.2f} blocks / minute")
    print("=" * 70)

    print("\nDetailed Per-Block Latency Breakdown:")
    print(f"{'Height':<8} | {'Block ID':<12} | {'HTTP Pub (s)':<14} | {'Total Block Time (s)':<20}")
    print("-" * 60)
    for r in results:
        print(f"{r['height']:<8} | {r['block_id']:<12} | {r['pub_latency']:<14.3f} | {r['total_time']:<20.3f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CDA Network Sequential Block Benchmark")
    parser.add_argument("--publisher", type=str, default="http://localhost:8080")
    parser.add_argument("--bootstrap", type=str, default="http://localhost:9200")
    parser.add_argument("--k", type=int, default=32)
    parser.add_argument("--count", type=int, default=10, help="Number of blocks to test")
    parser.add_argument("--timeout", type=float, default=180.0, help="Timeout per block in seconds (default: 180s)")
    parser.add_argument("--cell-size", type=int, default=64, help="Cell size in bytes (default: 64)")
    args = parser.parse_args()

    run_sequential_benchmark(args.publisher, args.bootstrap, args.k, args.count, args.timeout, cell_size=args.cell_size)
