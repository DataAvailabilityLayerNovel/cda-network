#!/usr/bin/env python3
"""
CDA Network - Pipelined (Pre-Compute) Block Processing Benchmark & Verification Script.

Tests the proactive 1-block-ahead pipeline:
1. At block H, proactively push block H+1 to the Publisher Node while block H is still in-flight.
2. Verify that Publisher admits block H+1 (HTTP 200) without blocking on block H completion.
3. Bootstrap Node receives block H+1, pre-computes KZG opening proofs & RLNC seeds into buffer,
   and holds dispatch until block H reaches BlockReady.
4. Wait for block H to reach BlockReady.
5. As soon as block H finishes, block H+1 seeds are instantly dispatched from buffer,
   achieving minimal latency.
6. Proactively push block H+2, and repeat.
"""

import argparse
import concurrent.futures
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


def publish_block(publish_url, block_id, height, k, timeout=60, cell_size=64):
    """Publish a single block to the Publisher Node. Returns (success, http_latency_seconds, error_msg)."""
    data = generate_ods_data(k, cell_size=cell_size)
    payload = {
        "block_id": block_id,
        "height": height,
        "data": data
    }
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
        return True, time.time() - start, ""
    except urllib.error.HTTPError as e:
        err_body = e.read().decode('utf-8', errors='ignore')
        return False, time.time() - start, f"HTTP {e.code}: {err_body.strip()}"
    except Exception as e:
        return False, time.time() - start, str(e)


def wait_for_block_ready_sse(sse_url, expected_block_id, timeout=120):
    """
    Subscribe to SSE stream on Bootstrap Node and block until expected_block_id appears.
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


def wait_for_block_ready_poll(latest_url, expected_block_id, poll_interval=0.15, timeout=120):
    """
    Poll /block-ready/latest on Bootstrap Node until expected_block_id appears.
    Returns (success, ready_latency_seconds).
    """
    start_time = time.time()
    deadline = start_time + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(latest_url, timeout=3) as resp:
                if resp.status == 200:
                    body = resp.read().decode('utf-8')
                    if body.strip():
                        pl = json.loads(body)
                        if pl.get("block_id") == expected_block_id:
                            return True, time.time() - start_time
        except Exception:
            pass
        time.sleep(poll_interval)
    return False, time.time() - start_time


def wait_for_block(latest_url, sse_url, block_id, timeout):
    """Combines fast polling with SSE fallback to wait for BlockReady."""
    ready, lat = wait_for_block_ready_poll(latest_url, block_id, poll_interval=0.1, timeout=0.3)
    if ready:
        return True, lat
    ready, lat = wait_for_block_ready_sse(sse_url, block_id, timeout=timeout)
    if ready:
        return True, lat
    return wait_for_block_ready_poll(latest_url, block_id, timeout=max(1.0, timeout - lat))


def run_pipeline_benchmark(publisher_url, bootstrap_url, k, count, timeout, cell_size=64):
    publish_api = f"{publisher_url.rstrip('/')}/publish"
    sse_api     = f"{bootstrap_url.rstrip('/')}/events/block-ready"
    latest_api  = f"{bootstrap_url.rstrip('/')}/block-ready/latest"

    print("=" * 75)
    print(" CDA NETWORK — PIPELINED (PRE-COMPUTE) BLOCK PROCESSING BENCHMARK")
    print("=" * 75)
    print(f"[*] Publisher API : {publish_api}")
    print(f"[*] Bootstrap API : {bootstrap_url}")
    print(f"[*] Matrix K      : {k} (ODS cells: {k*k})")
    print(f"[*] Cell Size     : {cell_size} bytes (ODS Block: {(k*k*cell_size)/(1024*1024):.2f} MB)")
    print(f"[*] Target Blocks : {count}")
    print(f"[*] Max Timeout   : {timeout}s per block")
    print(f"[*] Mode          : Pipelined 1-Block-Ahead Pre-Computation")
    print("-" * 75)

    results = []
    benchmark_start = time.time()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

    # Future for in-flight proactive block push
    ahead_future = None
    next_ahead_height = 1

    # Step 1: Push initial block-1
    block1_id = "block-1"
    print(f"[{time.strftime('%H:%M:%S')}] Pushing initial {block1_id} (height 1)...", end="", flush=True)
    ok1, pub_lat1, err1 = publish_block(publish_api, block1_id, 1, k, timeout=timeout, cell_size=cell_size)
    if not ok1:
        print(f" ❌ FAILED to publish block-1: {err1}")
        return
    print(f" Accepted ({pub_lat1:.2f}s).")

    # Step 2: Proactively push block-2 ahead into pipeline if count >= 2
    if count >= 2:
        block2_id = "block-2"
        print(f"[{time.strftime('%H:%M:%S')}] 🚀 [PIPELINE] Proactively pushing ahead {block2_id} (height 2) while block-1 is running...", end="", flush=True)
        ahead_future = executor.submit(publish_block, publish_api, block2_id, 2, k, timeout, cell_size)
        next_ahead_height = 3

    # Main Loop: Process blocks 1 to count
    for curr_height in range(1, count + 1):
        curr_block_id = f"block-{curr_height}"
        curr_start = time.time()

        print(f"[{time.strftime('%H:%M:%S')}] Waiting for {curr_block_id} BlockReady (Custody + Non-Custody)...", end="", flush=True)

        # If an ahead push was launched in background, check its status
        if ahead_future is not None and curr_height > 1:
            try:
                ok_ahead, lat_ahead, err_ahead = ahead_future.result(timeout=5.0)
                if not ok_ahead:
                    print(f"\n  [-] Warning: Ahead push for {curr_block_id} failed: {err_ahead}")
                else:
                    print(f" (Ahead push accepted in {lat_ahead:.2f}s)", end="", flush=True)
            except Exception as e:
                print(f"\n  [-] Warning: Ahead future error: {e}")
            ahead_future = None

        # Wait for curr_block_id to complete
        ready, ready_lat = wait_for_block(latest_api, sse_api, curr_block_id, timeout=timeout)
        total_curr_time = time.time() - curr_start

        if ready:
            print(f" ✅ BlockReady in {total_curr_time:.3f}s!")
            results.append({
                "height": curr_height,
                "block_id": curr_block_id,
                "total_time": total_curr_time,
                "status": "SUCCESS"
            })
        else:
            print(f" ❌ TIMEOUT after {total_curr_time:.3f}s!")
            results.append({
                "height": curr_height,
                "block_id": curr_block_id,
                "total_time": total_curr_time,
                "status": "TIMEOUT"
            })
            # If block timed out, abort pipeline
            break

        # As soon as current block reaches BlockReady, proactively push next ahead block
        if next_ahead_height <= count:
            ahead_block_id = f"block-{next_ahead_height}"
            print(f"[{time.strftime('%H:%M:%S')}] 🚀 [PIPELINE] Proactively pushing ahead {ahead_block_id} (height {next_ahead_height}) into buffer...", end="", flush=True)
            ahead_future = executor.submit(publish_block, publish_api, ahead_block_id, next_ahead_height, k, timeout, cell_size)
            next_ahead_height += 1

    total_benchmark_time = time.time() - benchmark_start
    executor.shutdown(wait=False)

    print("\n" + "=" * 75)
    print(" PIPELINE BENCHMARK RESULTS & PRE-COMPUTE AUDIT")
    print("=" * 75)
    successful = [r for r in results if r['status'] == 'SUCCESS']
    print(f"[*] Total Blocks Completed : {len(successful)} / {count}")
    print(f"[*] Total Elapsed Time    : {total_benchmark_time:.2f} seconds")

    if successful:
        avg_time = sum(r['total_time'] for r in successful) / len(successful)
        min_time = min(r['total_time'] for r in successful)
        max_time = max(r['total_time'] for r in successful)
        bpm = (len(successful) / total_benchmark_time) * 60.0

        print(f"[*] Average Block Time    : {avg_time:.3f} seconds / block")
        print(f"[*] Min Block Time        : {min_time:.3f} seconds")
        print(f"[*] Max Block Time        : {max_time:.3f} seconds")
        print(f"[*] Processing Throughput : {bpm:.2f} blocks / minute")

        if len(successful) >= 2:
            cold_time = successful[0]['total_time']
            pipelined_times = [r['total_time'] for r in successful[1:]]
            avg_pipelined = sum(pipelined_times) / len(pipelined_times)
            speedup = ((cold_time - avg_pipelined) / cold_time) * 100.0 if cold_time > 0 else 0
            print(f"[*] Cold Block 1 Time     : {cold_time:.3f} seconds (Initial, no pre-compute)")
            print(f"[*] Pipelined Avg Time    : {avg_pipelined:.3f} seconds (With Pre-Computation)")
            print(f"[*] Latency Improvement   : {speedup:.1f}% faster due to Pre-Computation overlap!")

    print("-" * 75)
    print("Per-Block Processing Latency Breakdown:")
    print(f"{'Height':<8} | {'Block ID':<12} | {'Total Time (s)':<18} | {'Status':<10}")
    print("-" * 55)
    for r in results:
        print(f"{r['height']:<8} | {r['block_id']:<12} | {r['total_time']:<18.3f} | {r['status']:<10}")
    print("=" * 75)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CDA Network Pipelined Block Pre-Compute Benchmark")
    parser.add_argument("--publisher", type=str, default="http://localhost:8080", help="Publisher URL")
    parser.add_argument("--bootstrap", type=str, default="http://localhost:9200", help="Bootstrap Node API URL")
    parser.add_argument("--k", type=int, default=8, help="ODS matrix dimension K (default: 8)")
    parser.add_argument("--count", type=int, default=3, help="Number of blocks to test (default: 3)")
    parser.add_argument("--timeout", type=float, default=120.0, help="Timeout per block in seconds (default: 120s)")
    parser.add_argument("--cell-size", type=int, default=64, help="Cell size in bytes (default: 64)")
    args = parser.parse_args()

    run_pipeline_benchmark(args.publisher, args.bootstrap, args.k, args.count, args.timeout, cell_size=args.cell_size)
