#!/usr/bin/env python3
"""
run_cda_isolated_matrix.py - Strict Sequential Isolated CDA Network Benchmark Runner.

Executes sequential block processing across K = [8, 16, 32, 64] with kPiece = 4:
- 20 blocks per scenario
- Strict sequential execution: pushes block H, waits for 100% custody BlockReady before block H+1
- Directly utilizes existing test: scripts/tests/benchmark/test_sequential_block_timing.py
- Measures: Average, Min, Max Block Time, Throughput, RS encode time, KZG proof time
"""

import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
import urllib.error

# Benchmark Scenarios
SCENARIOS = [8, 16, 32, 64]
KPIECE = 4
BLOCKS_PER_SCENARIO = 20
CELL_SIZE = 512
ACTIVE_COLS = 1
NUM_COLS = 8
STORES_PER_COL = 8

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def cleanup_processes(pids):
    """Terminate and kill processes."""
    for p in pids:
        try:
            p.terminate()
        except Exception:
            pass
    time.sleep(1)
    for p in pids:
        try:
            p.kill()
        except Exception:
            pass
    time.sleep(1)


def clean_data_dirs():
    """Remove runtime keys and local BadgerDB storage."""
    subprocess.run("rm -f store_*.key *.key", shell=True, cwd=REPO_ROOT)
    subprocess.run("rm -rf data/store_* data/light_* data/publisher data/bootstrap_*", shell=True, cwd=REPO_ROOT)


def wait_for_stores_registration(bootstrap_port=9200, expected_stores=8, timeout=40):
    """Wait until all store nodes register with the bootstrap node."""
    deadline = time.time() + timeout
    peers_url = f"http://localhost:{bootstrap_port}/bootstrap/peers"
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(peers_url, timeout=1) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode('utf-8'))
                    peers = data.get("peers", [])
                    if len(peers) >= expected_stores:
                        return True, len(peers)
        except Exception:
            pass
        time.sleep(0.5)
    return False, 0


def collect_metrics():
    """Collect RS encode duration from publisher and KZG proof duration from bootstrap."""
    rs_avg = 0.0
    kzg_avg = 0.0
    try:
        content = urllib.request.urlopen("http://localhost:8080/metrics", timeout=2).read().decode('utf-8')
        sum_val, count_val = 0.0, 0
        for line in content.splitlines():
            if line.startswith("cda_publisher_rs_encode_duration_seconds_sum"):
                sum_val = float(line.split()[1])
            elif line.startswith("cda_publisher_rs_encode_duration_seconds_count"):
                count_val = int(line.split()[1])
        if count_val > 0:
            rs_avg = (sum_val / count_val) * 1000.0  # ms
    except Exception:
        pass

    try:
        content = urllib.request.urlopen("http://localhost:9200/metrics", timeout=2).read().decode('utf-8')
        sum_val, count_val = 0.0, 0
        for line in content.splitlines():
            if line.startswith("cda_bootstrap_kzg_proof_duration_seconds_sum"):
                sum_val = float(line.split()[1])
            elif line.startswith("cda_bootstrap_kzg_proof_duration_seconds_count"):
                count_val = int(line.split()[1])
        if count_val > 0:
            kzg_avg = (sum_val / count_val) * 1000.0  # ms
    except Exception:
        pass

    return rs_avg, kzg_avg


def start_isolated_cluster(k_val, k_piece_val):
    """Starts publisher, bootstrap, and store nodes with optimal configuration per K."""
    clean_data_dirs()

    env = os.environ.copy()
    env["STORE_SHARDED_WORKERS"] = "24" if k_val >= 16 else "16"
    if k_val == 8:
        env["STORE_GOSSIP_BATCH_SIZE"] = "32"
    elif k_val == 16:
        env["STORE_GOSSIP_BATCH_SIZE"] = "48"
    elif k_val == 32:
        env["STORE_GOSSIP_BATCH_SIZE"] = "64"
    else:
        env["STORE_GOSSIP_BATCH_SIZE"] = "96"

    env["STORE_GOSSIP_BATCH_WORKERS"] = "2" if k_val <= 16 else "4"
    env["STORE_GOSSIP_BATCH_TICKER_MS"] = "10"
    env["STORE_DISSEMINATION_SEM"] = "32"
    env["BOOTSTRAP_SEEDING_SEM"] = "64"
    env["BOOTSTRAP_BATCH_CHUNK_SIZE"] = "64"
    if k_val >= 32:
        env["BOOTSTRAP_PROOF_GEN_SEM"] = "16" if k_val == 32 else "32"
    env["GOMAXPROCS"] = "4"

    processes = []
    n_dim = 2 * k_val
    cols_per_net_col = max(1, n_dim // NUM_COLS)

    boot_args = []
    # 1. Start Bootstraps
    for c in range(ACTIVE_COLS):
        boot_port = 9200 + c
        start_col = c * cols_per_net_col
        boot_args.append(f"{c}:http://localhost:{boot_port}")

        p_b = subprocess.Popen([
            "./bin/bootstrap",
            "-port", str(boot_port),
            "-col", str(start_col),
            "-publisher", "http://localhost:8080",
            "-k", str(k_val),
            "-k-piece", str(k_piece_val),
            "-prune-enable=true",
            "-prune-ttl", "15s"
        ], cwd=REPO_ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        processes.append(p_b)

    # 2. Start Store Nodes
    for c in range(ACTIVE_COLS):
        boot_port = 9200 + c
        start_col = c * cols_per_net_col
        for r in range(STORES_PER_COL):
            port = 9300 + c * STORES_PER_COL + r
            p_s = subprocess.Popen([
                "./bin/store",
                "-port", str(port),
                "-row", str(r),
                "-col", str(start_col),
                "-publisher", "http://localhost:8080",
                "-bootstrap", f"http://localhost:{boot_port}",
                "-k", str(k_val),
                "-k-piece", str(k_piece_val),
                "-num-cols", str(NUM_COLS),
                "-stores-per-col", str(STORES_PER_COL),
                "-prune-enable=true",
                "-prune-ttl", "15s"
            ], cwd=REPO_ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            processes.append(p_s)

    # 3. Start Publisher
    bootstrap_arg_str = ";".join(boot_args)
    p_pub = subprocess.Popen([
        "./bin/publisher",
        "-port", "8080",
        "-bootstrap", bootstrap_arg_str,
        "-k", str(k_val),
        "-k-piece", str(k_piece_val),
        "-num-cols", str(NUM_COLS),
        "-active-cols", str(ACTIVE_COLS)
    ], cwd=REPO_ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    processes.append(p_pub)

    # 4. Wait for store registration
    log(f"Waiting for store nodes to register with Bootstrap (K={k_val})...")
    ok, count = wait_for_stores_registration(bootstrap_port=9200, expected_stores=STORES_PER_COL, timeout=40)
    if not ok:
        log(f"⚠️ Warning: Only {count}/{STORES_PER_COL} store nodes registered before timeout!")
    else:
        log(f"✅ All {count} store nodes registered successfully.")

    # Mesh stabilization delay
    log("Waiting 5s for GossipSub mesh & topic subscriptions to stabilize...")
    time.sleep(5)

    return processes


def parse_timing_output(stdout_text):
    """Parses output from test_sequential_block_timing.py."""
    metrics = {
        "completed_blocks": 0,
        "total_elapsed_time": 0.0,
        "avg_block_time": 0.0,
        "min_block_time": 0.0,
        "max_block_time": 0.0,
        "blocks_per_minute": 0.0,
    }
    for line in stdout_text.splitlines():
        line = line.strip()
        if "Total Blocks Completed" in line:
            parts = line.split(":")
            if len(parts) > 1:
                sub = parts[1].split("/")[0].strip()
                metrics["completed_blocks"] = int(sub)
        elif "Total Elapsed Time" in line:
            parts = line.split(":")
            if len(parts) > 1:
                metrics["total_elapsed_time"] = float(parts[1].replace("seconds", "").strip())
        elif "Average Block Time" in line:
            parts = line.split(":")
            if len(parts) > 1:
                metrics["avg_block_time"] = float(parts[1].replace("seconds / block", "").replace("seconds", "").strip())
        elif "Min Block Time" in line:
            parts = line.split(":")
            if len(parts) > 1:
                metrics["min_block_time"] = float(parts[1].replace("seconds", "").strip())
        elif "Max Block Time" in line:
            parts = line.split(":")
            if len(parts) > 1:
                metrics["max_block_time"] = float(parts[1].replace("seconds", "").strip())
        elif "Processing Throughput" in line:
            parts = line.split(":")
            if len(parts) > 1:
                metrics["blocks_per_minute"] = float(parts[1].replace("blocks / minute", "").strip())
    return metrics


def run_benchmark_for_k(k_val):
    """Executes 20 blocks sequentially for a given K using test_sequential_block_timing.py."""
    log("=" * 80)
    log(f" STARTING BENCHMARK SCENARIO: K={k_val} (ODS {k_val}x{k_val}, EDS {2*k_val}x{2*k_val}) | {BLOCKS_PER_SCENARIO} BLOCKS")
    log("=" * 80)

    pids = start_isolated_cluster(k_val, KPIECE)

    # Run the existing benchmark script
    timing_script = os.path.join(REPO_ROOT, "scripts/tests/benchmark/test_sequential_block_timing.py")
    timeout_per_block = 120 if k_val <= 32 else 180
    cmd = [
        sys.executable, timing_script,
        "--publisher", "http://localhost:8080",
        "--bootstrap", "http://localhost:9200",
        "--k", str(k_val),
        "--count", str(BLOCKS_PER_SCENARIO),
        "--timeout", str(timeout_per_block),
        "--cell-size", str(CELL_SIZE)
    ]

    log(f"Executing: {' '.join(cmd)}")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    full_output = []
    for line in proc.stdout:
        print(line, end="", flush=True)
        full_output.append(line)
    proc.wait()
    stdout_text = "".join(full_output)

    # Collect prometheus metrics before killing processes
    rs_ms, kzg_ms = collect_metrics()

    # Clean up cluster
    cleanup_processes(pids)

    parsed = parse_timing_output(stdout_text)
    block_kb = (k_val * k_val * CELL_SIZE) / 1024.0
    elapsed = parsed["total_elapsed_time"] if parsed["total_elapsed_time"] > 0 else 1.0
    throughput_kb = (block_kb * parsed["completed_blocks"]) / elapsed

    return {
        "k": k_val,
        "completed": parsed["completed_blocks"],
        "target": BLOCKS_PER_SCENARIO,
        "total_time": parsed["total_elapsed_time"],
        "avg_time": parsed["avg_block_time"],
        "min_time": parsed["min_block_time"],
        "max_time": parsed["max_block_time"],
        "bpm": parsed["blocks_per_minute"],
        "throughput_kb": throughput_kb,
        "rs_ms": rs_ms,
        "kzg_ms": kzg_ms,
    }


def main():
    log("==========================================================================")
    log(" CDA NETWORK ISOLATED MATRIX BENCHMARK (K = 8 -> 64, kPiece = 4)")
    log(" Mode: Strictly Sequential | Target: 20 Blocks per Scenario")
    log("==========================================================================")

    # Clean any stale processes
    subprocess.run("pkill -9 publisher bootstrap store light 2>/dev/null || true", shell=True)
    time.sleep(1)

    summary_table = []
    os.makedirs(os.path.join(REPO_ROOT, "data/benchmarks"), exist_ok=True)

    for k in SCENARIOS:
        res = run_benchmark_for_k(k)
        summary_table.append(res)
        # Incremental save
        out_file = os.path.join(REPO_ROOT, "data/benchmarks/benchmark_isolated_k8_k64.json")
        with open(out_file, "w") as f:
            json.dump(summary_table, f, indent=2)
        time.sleep(3)

    print("\n" + "=" * 110)
    print(" CDA NETWORK ISOLATED BENCHMARK SUMMARY (kPiece = 4, 20 Blocks per Scenario)")
    print("=" * 110)
    print(f"{'K':<5} | {'ODS Dimension':<15} | {'EDS Dimension':<16} | {'Avg Time (s)':<13} | {'Min (s)':<9} | {'Max (s)':<9} | {'Throughput':<14} | {'RS Encode':<11} | {'KZG Proof':<11}")
    print("-" * 110)
    for s in summary_table:
        k = s['k']
        ods_str = f"{k}x{k} ({k*k} cells)"
        eds_str = f"{2*k}x{2*k} ({4*k*k} cells)"
        th_str = f"{s['throughput_kb']:.1f} KB/s" if s['throughput_kb'] < 1024 else f"{s['throughput_kb']/1024:.2f} MB/s"
        print(f"{k:<5} | {ods_str:<15} | {eds_str:<16} | {s['avg_time']:<13.3f} | {s['min_time']:<9.3f} | {s['max_time']:<9.3f} | {th_str:<14} | {s['rs_ms']:<9.2f}ms | {s['kzg_ms']:<9.2f}ms")
    print("=" * 110)
    print(f"\n[+] Raw results saved to: {out_file}\n")


if __name__ == '__main__':
    main()
