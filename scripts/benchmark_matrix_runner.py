#!/usr/bin/env python3
"""
CDA Network - Multi-Configuration Benchmark Matrix Runner.

Automates running benchmarks across different system configurations:
1. Matrix Dimension K (e.g. 8, 16, 32, 64)
2. Processing Mode (Sequential vs 1-Block-Ahead Pipeline)
3. Concurrency & Batching Parameters (GossipBatchSize, Workers, Semaphores)
4. Hardware / Resource Constraints (CPU quotas, GOMAXPROCS)

Collects detailed latency, throughput, and system resource metrics,
saving raw data and summaries to JSON in data/benchmarks/.
"""

import argparse
import datetime
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

# Ensure we run from repository root
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
os.chdir(REPO_ROOT)


def log(msg):
    now = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}", flush=True)


def wait_for_publisher(url="http://localhost:8080", timeout=45):
    """Wait until Publisher Node is healthy."""
    deadline = time.time() + timeout
    health_url = f"{url.rstrip('/')}/health"
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(health_url, timeout=2) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode('utf-8'))
                    if data.get("status") == "healthy":
                        return True
        except Exception:
            pass
        time.sleep(1)
    return False


def wait_for_stores_registration(bootstrap_port=9200, min_stores=4, timeout=45):
    """Wait until Store Nodes register with Bootstrap Node."""
    deadline = time.time() + timeout
    peers_url = f"http://localhost:{bootstrap_port}/bootstrap/peers"
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(peers_url, timeout=2) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode('utf-8'))
                    peers = data.get("peers", [])
                    if len(peers) >= min_stores:
                        return True, len(peers)
        except Exception:
            pass
        time.sleep(1)
    return False, 0


def run_cmd(cmd, check=True, capture_output=False):
    """Run shell command with logging."""
    if isinstance(cmd, list):
        cmd_str = " ".join(cmd)
    else:
        cmd_str = cmd
    res = subprocess.run(cmd_str, shell=True, check=check, text=True,
                         stdout=subprocess.PIPE if capture_output else None,
                         stderr=subprocess.PIPE if capture_output else None)
    return res


def stop_and_cleanup_cluster():
    """Tear down docker compose environment and clean volumes."""
    log("Cleaning up previous Docker containers and volumes...")
    run_cmd("docker compose -f docker-compose.json down -v --remove-orphans 2>/dev/null || true", check=False)
    run_cmd("bash scripts/cleanup.sh 2>/dev/null || true", check=False)


def collect_docker_stats():
    """Sample current CPU and memory usage from running containers."""
    try:
        cmd = 'docker stats --no-stream --format "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"'
        res = subprocess.run(cmd, shell=True, text=True, capture_output=True, timeout=5)
        stats = {}
        for line in res.stdout.strip().splitlines():
            parts = line.split("\t")
            if len(parts) >= 3:
                stats[parts[0]] = {
                    "cpu_pct": parts[1],
                    "mem_usage": parts[2]
                }
        return stats
    except Exception as e:
        return {"error": str(e)}


def collect_store_metrics(port=9300):
    """Collects actual BadgerDB disk size and Prometheus storage metrics from Store Node."""
    metrics = {
        "disk_usage_bytes": 0,
        "disk_usage_kb": 0.0,
        "db_size_bytes": 0,
        "custody_pieces": 0,
        "recoded_pieces": 0,
        "total_pieces": 0,
    }
    store_dir = f"data/store_{port}"
    if os.path.exists(store_dir):
        total_b = 0
        for dirpath, _, filenames in os.walk(store_dir):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_b += os.path.getsize(fp)
        metrics["disk_usage_bytes"] = total_b
        metrics["disk_usage_kb"] = round(total_b / 1024.0, 2)
    try:
        url = f"http://localhost:{port}/metrics"
        with urllib.request.urlopen(url, timeout=2) as resp:
            text = resp.read().decode("utf-8")
            for line in text.splitlines():
                if line.startswith("cda_store_db_size_bytes"):
                    metrics["db_size_bytes"] = float(line.split()[1])
                elif line.startswith("cda_store_custody_pieces_count"):
                    metrics["custody_pieces"] = int(float(line.split()[1]))
                elif line.startswith("cda_store_recoded_pieces_count"):
                    metrics["recoded_pieces"] = int(float(line.split()[1]))
                elif line.startswith("cda_store_linear_independent_pieces_count"):
                    metrics["total_pieces"] = int(float(line.split()[1]))
    except Exception:
        pass
    return metrics


def execute_benchmark_scenario(scenario, blocks=3, timeout_per_block=120, dry_run=False, skip_docker=False):
    """
    Executes a single benchmark scenario.
    Returns result dictionary.
    """
    s_id = scenario["id"]
    k = scenario.get("k", 8)
    mode = scenario.get("mode", "pipeline")
    num_cols = scenario.get("num_cols", 16)
    active_cols = scenario.get("active_cols", 1)
    stores_per_col = scenario.get("stores_per_col", 8)
    lights = scenario.get("lights", 1)
    k_piece = scenario.get("k_piece", 4)

    # Concurrency and tuning options
    batch_size = scenario.get("store_gossip_batch_size")
    batch_workers = scenario.get("store_gossip_batch_workers")
    batch_ticker = scenario.get("store_gossip_batch_ticker_ms")
    dissem_sem = scenario.get("store_dissemination_sem")
    fallback_sem = scenario.get("store_fallback_pull_sem")
    proof_sem = scenario.get("bootstrap_proof_gen_sem")
    seeding_sem = scenario.get("bootstrap_seeding_sem")
    publisher_max_in_flight = scenario.get("publisher_max_in_flight")
    gomaxprocs = scenario.get("gomaxprocs")
    cpus = scenario.get("cpus")

    log("=" * 80)
    log(f"RUNNING SCENARIO: {s_id}")
    log(f"Config: K={k}, Piece={k_piece}, Mode={mode}, BatchSize={batch_size}, Workers={batch_workers}, ProofSem={proof_sem}, Cores={cpus or 'host'}")
    log("=" * 80)

    if dry_run:
        log("[DRY-RUN] Scenario skipped (dry run mode).")
        return {
            "scenario": scenario,
            "status": "DRY_RUN",
            "timestamp": datetime.datetime.utcnow().isoformat()
        }

    if not skip_docker:
        # Step 1: Clean up
        stop_and_cleanup_cluster()

        # Step 2: Generate docker-compose.json
        compose_cmd = [
            "python3", "scripts/generate_compose.py",
            "-k", str(k),
            "-p", str(k_piece),
            "-c", str(active_cols),
            "-n", str(num_cols),
            "-s", str(stores_per_col),
            "-l", str(lights),
            "--crash-on-fail"
        ]
        if batch_size is not None:
            compose_cmd.extend(["--store-gossip-batch-size", str(batch_size)])
        if batch_workers is not None:
            compose_cmd.extend(["--store-gossip-batch-workers", str(batch_workers)])
        if batch_ticker is not None:
            compose_cmd.extend(["--store-gossip-batch-ticker-ms", str(batch_ticker)])
        if dissem_sem is not None:
            compose_cmd.extend(["--store-dissemination-sem", str(dissem_sem)])
        if fallback_sem is not None:
            compose_cmd.extend(["--store-fallback-pull-sem", str(fallback_sem)])
        if proof_sem is not None:
            compose_cmd.extend(["--bootstrap-proof-gen-sem", str(proof_sem)])
        if seeding_sem is not None:
            compose_cmd.extend(["--bootstrap-seeding-sem", str(seeding_sem)])
        if publisher_max_in_flight is not None:
            compose_cmd.extend(["--publisher-max-in-flight", str(publisher_max_in_flight)])
        if gomaxprocs is not None:
            compose_cmd.extend(["--gomaxprocs", str(gomaxprocs)])
        if cpus is not None:
            compose_cmd.extend(["--cpus", str(cpus)])

        log(f"Generating compose: {' '.join(compose_cmd)}")
        run_cmd(compose_cmd)

        # Step 3: Build and Up
        log("Starting Docker cluster containers...")
        run_cmd("docker compose -f docker-compose.json build")
        run_cmd("docker compose -f docker-compose.json up -d")

        # Step 4: Wait for services
        log("Waiting for Publisher node health...")
        if not wait_for_publisher():
            log("❌ Publisher failed to start!")
            return {"scenario": scenario, "status": "PUBLISHER_UNHEALTHY"}

        log(f"Waiting for {stores_per_col} store nodes to register with Bootstrap :9200...")
        reg_ok, count = wait_for_stores_registration(bootstrap_port=9200, min_stores=stores_per_col)
        if not reg_ok:
            log(f"❌ Store nodes failed to register (registered: {count}/{stores_per_col})!")
            return {"scenario": scenario, "status": "STORES_NOT_REGISTERED"}
        log(f"✅ All {count} store nodes registered successfully.")

        # Allow P2P mesh 2 seconds to settle
        time.sleep(2)

    # Step 5: Execute benchmark timing
    cell_size = scenario.get("cell_size", 64)
    timing_script = "scripts/test_pipeline_block_timing.py" if mode == "pipeline" else "scripts/test_sequential_block_timing.py"
    cmd = [
        "python3", timing_script,
        "--publisher", "http://localhost:8080",
        "--bootstrap", "http://localhost:9200",
        "--k", str(k),
        "--count", str(blocks),
        "--timeout", str(timeout_per_block),
        "--cell-size", str(cell_size)
    ]

    log(f"Executing timing probe: {' '.join(cmd)}")
    start_wall = time.time()
    res = run_cmd(cmd, check=False, capture_output=True)
    elapsed_wall = time.time() - start_wall

    output_lines = res.stdout if res.stdout else ""
    stderr_lines = res.stderr if res.stderr else ""
    print(output_lines)

    # Capture container stats and Store Node storage metrics
    docker_stats = collect_docker_stats() if not skip_docker else {}
    store_metrics = collect_store_metrics() if not skip_docker else {}

    # Parse timing results
    parsed = parse_benchmark_output(output_lines)
    parsed["wall_time_seconds"] = elapsed_wall
    parsed["exit_code"] = res.returncode
    parsed["docker_stats"] = docker_stats
    parsed["store_metrics"] = store_metrics
    parsed["scenario"] = scenario
    parsed["timestamp"] = datetime.datetime.utcnow().isoformat()
    parsed["status"] = "SUCCESS" if res.returncode == 0 and parsed.get("completed_blocks", 0) == blocks else "FAILED"

    if not skip_docker:
        stop_and_cleanup_cluster()

    return parsed


def parse_benchmark_output(text):
    """Parses stdout of test_pipeline_block_timing.py / test_sequential_block_timing.py."""
    metrics = {
        "completed_blocks": 0,
        "total_elapsed_time": 0.0,
        "avg_block_time": 0.0,
        "min_block_time": 0.0,
        "max_block_time": 0.0,
        "blocks_per_minute": 0.0,
        "cold_block_time": 0.0,
        "pipelined_avg_time": 0.0,
        "speedup_pct": 0.0,
        "per_block": []
    }
    for line in text.splitlines():
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
        elif "Cold Block 1 Time" in line:
            parts = line.split(":")
            if len(parts) > 1:
                val = parts[1].split("(")[0].replace("seconds", "").strip()
                metrics["cold_block_time"] = float(val)
        elif "Pipelined Avg Time" in line:
            parts = line.split(":")
            if len(parts) > 1:
                val = parts[1].split("(")[0].replace("seconds", "").strip()
                metrics["pipelined_avg_time"] = float(val)
        elif "Latency Improvement" in line:
            parts = line.split(":")
            if len(parts) > 1:
                val = parts[1].split("%")[0].strip()
                metrics["speedup_pct"] = float(val)

    return metrics


def build_scenario_matrix(matrix_type="baseline", custom_k="8,16"):
    """Generates lists of scenarios based on experiment phase."""
    scenarios = []

    if matrix_type == "baseline":
        # Baseline sweep: K in {8, 16, 32} with pipeline vs sequential
        k_values = [int(x.strip()) for x in custom_k.split(",") if x.strip()]
        for k in k_values:
            for mode in ["pipeline", "sequential"]:
                scenarios.append({
                    "id": f"baseline_k{k}_{mode}",
                    "k": k,
                    "mode": mode,
                    "cols": 1,
                    "stores_per_col": 4,
                    "lights": 1
                })

    elif matrix_type == "batching":
        # Store GossipSub batch size & worker sweep at K=16/32
        for batch_size in [24, 48, 64, 96]:
            for workers in [1, 2, 4]:
                scenarios.append({
                    "id": f"batch_b{batch_size}_w{workers}",
                    "k": 16,
                    "mode": "pipeline",
                    "store_gossip_batch_size": batch_size,
                    "store_gossip_batch_workers": workers,
                    "cols": 1,
                    "stores_per_col": 4,
                    "lights": 1
                })

    elif matrix_type == "bootstrap_sem":
        # Bootstrap proof gen semaphore sweep at K=32/64
        for sem in [8, 16, 32, None]:
            sem_name = f"sem{sem}" if sem else "unbounded"
            scenarios.append({
                "id": f"boot_proof_{sem_name}",
                "k": 32,
                "mode": "pipeline",
                "bootstrap_proof_gen_sem": sem,
                "cols": 1,
                "stores_per_col": 4,
                "lights": 1
            })

    elif matrix_type == "cpu_cores":
        # Resource allocation: 4, 8, 12 cores limit
        for cpus in ["4", "8", "12"]:
            scenarios.append({
                "id": f"cpu_quota_{cpus}cores",
                "k": 16,
                "mode": "pipeline",
                "cpus": cpus,
                "num_cols": 16,
                "active_cols": 1,
                "stores_per_col": 4,
                "lights": 1
            })

    elif matrix_type == "opt_k64":
        # Specific request: K=64, num_cols=16 (8 data cols per net col), stores_per_col=8, active_cols=1
        # Phase 1: K_piece sweep (2, 4, 8) with Pipeline baseline
        for p in [2, 4, 8]:
            scenarios.append({
                "id": f"opt_k64_p{p}_pipe_base",
                "k": 64,
                "k_piece": p,
                "mode": "pipeline",
                "num_cols": 16,
                "active_cols": 1,
                "stores_per_col": 8,
                "lights": 1
            })

        # Phase 2: Sequential comparison for p=4
        scenarios.append({
            "id": "opt_k64_p4_seq_base",
            "k": 64,
            "k_piece": 4,
            "mode": "sequential",
            "num_cols": 16,
            "active_cols": 1,
            "stores_per_col": 8,
            "lights": 1
        })

        # Phase 3: Store Gossip Batch size and workers tuning
        for bsize in [64, 96]:
            for w in [2, 4]:
                scenarios.append({
                    "id": f"opt_k64_p4_b{bsize}_w{w}",
                    "k": 64,
                    "k_piece": 4,
                    "mode": "pipeline",
                    "store_gossip_batch_size": bsize,
                    "store_gossip_batch_workers": w,
                    "num_cols": 16,
                    "active_cols": 1,
                    "stores_per_col": 8,
                    "lights": 1
                })

        # Phase 4: Bootstrap Proof Gen Semaphore tuning
        for psem in [16, 32]:
            scenarios.append({
                "id": f"opt_k64_p4_proofsem{psem}",
                "k": 64,
                "k_piece": 4,
                "mode": "pipeline",
                "bootstrap_proof_gen_sem": psem,
                "num_cols": 16,
                "active_cols": 1,
                "stores_per_col": 8,
                "lights": 1
            })

        # Phase 5: CPU Cores limit
        for cpus in ["8", "12"]:
            scenarios.append({
                "id": f"opt_k64_p4_cores{cpus}",
                "k": 64,
                "k_piece": 4,
                "mode": "pipeline",
                "cpus": cpus,
                "num_cols": 16,
                "active_cols": 1,
                "stores_per_col": 8,
                "lights": 1
            })

        # Phase 6: K_piece sweep with Optimal Configuration (Batch=96, W=2, ProofSem=16, Cores=8, MaxInFlight=2)
        for p in [2, 4, 8]:
            scenarios.append({
                "id": f"opt_k64_optconf_p{p}",
                "k": 64,
                "k_piece": p,
                "mode": "pipeline",
                "store_gossip_batch_size": 96,
                "store_gossip_batch_workers": 2,
                "bootstrap_proof_gen_sem": 16,
                "publisher_max_in_flight": 2,
                "cpus": "8",
                "num_cols": 16,
                "active_cols": 1,
                "stores_per_col": 8,
                "lights": 1
            })

        # Phase 7: Approach B - Cell 512B with K_piece=16 (Fragment=32B matching Fr Scalar)
        scenarios.append({
            "id": "opt_k64_512b_p16",
            "k": 64,
            "k_piece": 16,
            "cell_size": 512,
            "mode": "pipeline",
            "store_gossip_batch_size": 96,
            "store_gossip_batch_workers": 2,
            "bootstrap_proof_gen_sem": 16,
            "publisher_max_in_flight": 2,
            "cpus": "8",
            "num_cols": 16,
            "active_cols": 1,
            "stores_per_col": 8,
            "lights": 1
        })

    return scenarios


def main():
    parser = argparse.ArgumentParser(description="CDA Network Multi-Configuration Benchmark Matrix Runner")
    parser.add_argument("--type", type=str, default="baseline", choices=["baseline", "batching", "bootstrap_sem", "cpu_cores", "opt_k64", "custom"],
                        help="Benchmark matrix type to run")
    parser.add_argument("--matrix-k", type=str, default="8,16", help="Comma-separated K values (default: 8,16)")
    parser.add_argument("--blocks", type=int, default=3, help="Number of blocks per benchmark run (default: 3)")
    parser.add_argument("--timeout", type=int, default=180, help="Per-block timeout in seconds (default: 180)")
    parser.add_argument("--filter", type=str, default=None, help="Filter scenario IDs by substring")
    parser.add_argument("--append", action="store_true", help="Append to existing benchmark results")
    parser.add_argument("--dry-run", action="store_true", help="Print scenarios without executing")
    parser.add_argument("--skip-docker", action="store_true", help="Run against already active cluster (single scenario)")
    parser.add_argument("--out-dir", type=str, default="data/benchmarks", help="Output directory for results")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    scenarios = build_scenario_matrix(args.type, args.matrix_k)
    if args.filter:
        filters = [f.strip() for f in args.filter.split(",") if f.strip()]
        scenarios = [s for s in scenarios if any(f in s["id"] for f in filters)]

    log(f"Loaded {len(scenarios)} benchmark scenarios for plan '{args.type}'.")

    latest_file = os.path.join(args.out_dir, "benchmark_latest.json")
    all_results = []
    if args.append and os.path.exists(latest_file):
        try:
            with open(latest_file, "r") as f:
                all_results = json.load(f)
            log(f"Loaded {len(all_results)} existing results from {latest_file}.")
        except Exception:
            all_results = []

    run_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = os.path.join(args.out_dir, f"benchmark_{args.type}_{run_timestamp}.json")

    for idx, s in enumerate(scenarios, 1):
        log(f"\n--- Scenario [{idx}/{len(scenarios)}]: {s['id']} ---")
        res = execute_benchmark_scenario(
            s,
            blocks=args.blocks,
            timeout_per_block=args.timeout,
            dry_run=args.dry_run,
            skip_docker=args.skip_docker
        )
        all_results.append(res)

        # Incrementally save results
        with open(out_file, "w") as f:
            json.dump(all_results, f, indent=2)
        with open(latest_file, "w") as f:
            json.dump(all_results, f, indent=2)

    log("\n" + "=" * 80)
    log(f"BENCHMARK COMPLETED: {len(all_results)} scenarios executed.")
    log(f"Results saved to: {out_file}")
    log(f"Latest link: {latest_file}")
    log("=" * 80)


if __name__ == "__main__":
    main()
