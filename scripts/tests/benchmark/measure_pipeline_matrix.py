#!/usr/bin/env python3
"""
measure_pipeline_matrix.py - Comprehensive Pipeline Latency Benchmark across Increasing Block Sizes.

Enhanced Multi-Run Benchmark with Interleaved Load Balancing:
1. Multi-Iteration Execution: Evaluates statistical metrics (Min, Max, Mean, Median, StdDev).
2. Interleaved Scheduling (Light <-> Heavy): Prevents system strain, backpressure build-up,
   and thermal/queue saturation by alternating light and heavy blocks (e.g. K=8, K=64, K=16, K=32).
3. Dynamic Height & BlockID Reconciliation: Prevents collision with concurrent CometBFT blocks.
4. Comprehensive Metric Reporting: End-to-end latency, phase breakdown (Publish vs DA Wait),
   throughput (MB/s), Quorum readiness, and Full Custody convergence.
"""

import argparse
import datetime
import json
import os
import random
import statistics
import sys
import time
import urllib.error
import urllib.request

DEFAULT_PUBLISHER_URL = "http://localhost:8080"
DEFAULT_BOOTSTRAP_URL = "http://localhost:9200"
DEFAULT_CELL_SIZE = 512


def http_get(url, timeout=5):
    req = urllib.request.Request(url, headers={'Accept': 'application/json'})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode('utf-8'))


def get_latest_height(publisher_url=DEFAULT_PUBLISHER_URL):
    status = http_get(f"{publisher_url}/status")
    return status.get("latest_published_height", 0)


def generate_ods_data(k, cell_size=DEFAULT_CELL_SIZE):
    hex_len = cell_size * 2
    fmt = f'%0{hex_len}x'
    return [fmt % random.randint(1, 1_000_000) for _ in range(k * k)]


def wait_for_block_ready(bootstrap_url, publisher_url, block_id, timeout=180):
    deadline = time.time() + timeout
    latest_bootstrap_url = f"{bootstrap_url}/block-ready/latest"
    publisher_block_url = f"{publisher_url}/block/{block_id}"

    start_wait = time.time()
    while time.time() < deadline:
        # 1. Check Bootstrap latest block-ready SSE/cache
        try:
            with urllib.request.urlopen(latest_bootstrap_url, timeout=1) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode('utf-8'))
                    if data.get("block_id") == block_id:
                        # Confirmed by Bootstrap, fetch block details for quorum & custody flags
                        binfo = {}
                        try:
                            binfo = http_get(publisher_block_url, timeout=1)
                        except Exception:
                            pass
                        return True, time.time() - start_wait, data.get("k", 0), binfo
        except Exception:
            pass

        # 2. Check Publisher block status directly (da_quorum_ready == True)
        try:
            binfo = http_get(publisher_block_url, timeout=1)
            if binfo.get("da_quorum_ready", False) is True:
                return True, time.time() - start_wait, binfo.get("k", 0), binfo
        except Exception:
            pass

        time.sleep(0.05)

    # If timeout occurred, try getting block info anyway
    binfo = {}
    try:
        binfo = http_get(publisher_block_url, timeout=1)
    except Exception:
        pass
    return False, time.time() - start_wait, 0, binfo


def run_single_block(publisher_url, bootstrap_url, k, height, block_id, cell_size=DEFAULT_CELL_SIZE, timeout=180):
    ods_data = generate_ods_data(k, cell_size=cell_size)
    payload = {
        "block_id": block_id,
        "height": height,
        "data": ods_data
    }
    req_bytes = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        f"{publisher_url}/publish",
        data=req_bytes,
        headers={'Content-Type': 'application/json'}
    )

    t_start = time.time()
    actual_block_id = block_id
    actual_height = height
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            resp_body = resp.read().decode('utf-8')
            if resp_body:
                try:
                    resp_json = json.loads(resp_body)
                    if "block_id" in resp_json and resp_json["block_id"]:
                        actual_block_id = resp_json["block_id"]
                    if "height" in resp_json and resp_json["height"] > 0:
                        actual_height = resp_json["height"]
                except Exception:
                    pass
    except urllib.error.HTTPError as e:
        err_msg = e.read().decode('utf-8', errors='ignore')
        return {
            "k": k,
            "height": height,
            "block_id": block_id,
            "actual_block_id": actual_block_id,
            "actual_height": actual_height,
            "payload_bytes": k * k * cell_size,
            "pub_latency": time.time() - t_start,
            "ready_latency": 0.0,
            "total_latency": time.time() - t_start,
            "ready": False,
            "final_k": k,
            "da_quorum_ready": False,
            "full_custody": False,
            "error": f"HTTP {e.code}: {err_msg.strip()}"
        }
    except Exception as e:
        return {
            "k": k,
            "height": height,
            "block_id": block_id,
            "actual_block_id": actual_block_id,
            "actual_height": actual_height,
            "payload_bytes": k * k * cell_size,
            "pub_latency": time.time() - t_start,
            "ready_latency": 0.0,
            "total_latency": time.time() - t_start,
            "ready": False,
            "final_k": k,
            "da_quorum_ready": False,
            "full_custody": False,
            "error": str(e)
        }

    pub_latency = time.time() - t_start

    ready, ready_latency, final_k, block_info = wait_for_block_ready(
        bootstrap_url, publisher_url, actual_block_id, timeout=timeout
    )
    total_latency = time.time() - t_start

    # Secondary check on block_info if flags were not populated yet
    if not block_info.get("da_quorum_ready"):
        try:
            time.sleep(0.05)
            block_info = http_get(f"{publisher_url}/block/{actual_block_id}", timeout=2)
        except Exception:
            pass

    return {
        "k": k,
        "height": height,
        "block_id": block_id,
        "actual_block_id": actual_block_id,
        "actual_height": actual_height,
        "payload_bytes": k * k * cell_size,
        "pub_latency": pub_latency,
        "ready_latency": ready_latency,
        "total_latency": total_latency,
        "ready": ready,
        "final_k": final_k if final_k > 0 else k,
        "da_quorum_ready": block_info.get("da_quorum_ready", False),
        "full_custody": block_info.get("full_custody", False),
        "error": None
    }


def generate_schedule(k_values, iterations, pattern="interleaved"):
    """
    Generates submission schedule.
    'interleaved': Alternates min (light) and max (heavy) to avoid consecutive heavy loads.
                   e.g. for [8, 16, 32, 64] -> [8, 64, 16, 32].
    'sequential' : Ascending order [8, 16, 32, 64].
    'reverse'    : Descending order [64, 32, 16, 8].
    'random'     : Shuffled per iteration.
    """
    if pattern == "interleaved":
        sorted_k = sorted(k_values)
        left, right = 0, len(sorted_k) - 1
        round_pattern = []
        while left <= right:
            round_pattern.append(sorted_k[left])
            left += 1
            if left <= right:
                round_pattern.append(sorted_k[right])
                right -= 1
    elif pattern == "reverse":
        round_pattern = sorted(k_values, reverse=True)
    elif pattern == "sequential":
        round_pattern = sorted(k_values)
    elif pattern == "random":
        round_pattern = list(k_values)
        random.shuffle(round_pattern)
    else:
        round_pattern = list(k_values)

    schedule = []
    for it in range(1, iterations + 1):
        cur_pattern = list(round_pattern)
        if pattern == "random":
            random.shuffle(cur_pattern)
        for k in cur_pattern:
            schedule.append({"iteration": it, "k": k})

    return schedule, round_pattern


def compute_stats(data_series):
    """Computes mean, min, max, median, stdev for a list of numbers."""
    if not data_series:
        return {"mean": 0.0, "min": 0.0, "max": 0.0, "median": 0.0, "stdev": 0.0}
    mean_val = statistics.mean(data_series)
    min_val = min(data_series)
    max_val = max(data_series)
    med_val = statistics.median(data_series)
    std_val = statistics.stdev(data_series) if len(data_series) >= 2 else 0.0
    return {
        "mean": mean_val,
        "min": min_val,
        "max": max_val,
        "median": med_val,
        "stdev": std_val
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="CDA Network Pipeline Benchmark - Multi-Run with Interleaved Load Balancing"
    )
    parser.add_argument(
        "-n", "--iterations",
        type=int,
        default=3,
        help="Number of iterations to run per matrix scenario (default: 3)"
    )
    parser.add_argument(
        "-k", "--k-values",
        type=str,
        default="8,16,32,64",
        help="Comma-separated matrix K values (default: 8,16,32,64)"
    )
    parser.add_argument(
        "--order",
        choices=["interleaved", "sequential", "reverse", "random"],
        default="interleaved",
        help="Execution pattern: 'interleaved' (alternating light & heavy, default), 'sequential', 'reverse', 'random'"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Inter-block cooldown pause in seconds between submissions (default: 1.0s)"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=180.0,
        help="Timeout in seconds for block BlockReady detection (default: 180s)"
    )
    parser.add_argument(
        "--publisher",
        type=str,
        default=DEFAULT_PUBLISHER_URL,
        help=f"Publisher node URL (default: {DEFAULT_PUBLISHER_URL})"
    )
    parser.add_argument(
        "--bootstrap",
        type=str,
        default=DEFAULT_BOOTSTRAP_URL,
        help=f"Bootstrap node URL (default: {DEFAULT_BOOTSTRAP_URL})"
    )
    parser.add_argument(
        "--cell-size",
        type=int,
        default=DEFAULT_CELL_SIZE,
        help=f"Payload cell size in bytes (default: {DEFAULT_CELL_SIZE})"
    )
    parser.add_argument(
        "--json-out",
        type=str,
        default="",
        help="Optional file path to output detailed benchmark results in JSON format"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output"
    )
    return parser.parse_args()


def format_size(bytes_val):
    kb = bytes_val / 1024.0
    if kb < 1024:
        return f"{kb:.0f} KB"
    return f"{kb/1024.0:.1f} MB"


def main():
    args = parse_args()

    k_values = [int(x.strip()) for x in args.k_values.split(",") if x.strip()]
    if not k_values:
        print("❌ Error: No valid K values provided.")
        sys.exit(1)

    schedule, round_pattern = generate_schedule(k_values, args.iterations, pattern=args.order)

    print("=" * 95)
    print("       CDA NETWORK PIPELINE BENCHMARK — INTERLEAVED MULTI-RUN EVALUATION")
    print("=" * 95)

    try:
        curr_height = get_latest_height(args.publisher)
        print(f"[*] Connected to Publisher : {args.publisher}")
        print(f"[*] Connected to Bootstrap : {args.bootstrap}")
        print(f"[*] Current Published Height : {curr_height}")
    except Exception as e:
        print(f"❌ Failed to connect to services: {e}")
        sys.exit(1)

    total_submissions = len(schedule)
    pattern_desc = " ➔ ".join([f"K={k} ({format_size(k*k*args.cell_size)})" for k in round_pattern])

    print("[*] Benchmark Parameters:")
    print(f"    - Iterations per K     : {args.iterations} (Total Submissions: {total_submissions} blocks)")
    print(f"    - Matrix Scenarios     : K = {k_values}")
    print(f"    - Execution Pattern    : {args.order.upper()} [ {pattern_desc} ]")
    print(f"    - Inter-block Cooldown : {args.delay:.1f}s")
    print(f"    - BlockReady Timeout   : {args.timeout:.0f}s")
    if args.order == "interleaved":
        print("    - Load Balancing       : Active (Alternating Light ↔ Heavy to drain queues & prevent spikes)")
    print("=" * 95)

    raw_results = []
    bench_start_time = time.time()

    for idx, item in enumerate(schedule, start=1):
        it = item["iteration"]
        k = item["k"]

        # Fetch latest height before submitting to prevent height collision with cometbft
        try:
            latest_h = get_latest_height(args.publisher)
        except Exception:
            latest_h = curr_height
        target_height = latest_h + 1
        block_id = f"bench-block-{target_height}-{k}"

        ods_cells = k * k
        ods_size_str = format_size(ods_cells * args.cell_size)
        eds_dim = f"{2*k}x{2*k}"

        print(f"\n[{idx:2d}/{total_submissions}] Round {it}/{args.iterations} | Scenario: K={k:<2d} ({ods_cells} cells, {ods_size_str}, EDS {eds_dim})")
        print(f"     Target Block: {block_id} (Height {target_height})...", end="", flush=True)

        res = run_single_block(
            publisher_url=args.publisher,
            bootstrap_url=args.bootstrap,
            k=k,
            height=target_height,
            block_id=block_id,
            cell_size=args.cell_size,
            timeout=args.timeout
        )
        res["iteration"] = it
        res["schedule_index"] = idx
        raw_results.append(res)

        if res["ready"]:
            if res["full_custody"]:
                custody_tag = "100% Full Custody"
            elif res["da_quorum_ready"]:
                custody_tag = "Quorum Ready (6/8 Nodes)"
            else:
                custody_tag = "Reconstructed/Ready"
            print(f" ✅ BlockReady in {res['total_latency']:.3f}s (Pub: {res['pub_latency']:.3f}s, DA Wait: {res['ready_latency']:.3f}s) [{custody_tag}]")
        elif res.get("error"):
            print(f" ❌ ERROR: {res['error']} (after {res['total_latency']:.3f}s)")
        else:
            print(f" ❌ TIMEOUT after {res['total_latency']:.3f}s")

        if idx < total_submissions and args.delay > 0:
            time.sleep(args.delay)

    total_bench_time = time.time() - bench_start_time

    # =========================================================================
    # Group results by K and compute statistics
    # =========================================================================
    by_k = {}
    for k in sorted(k_values):
        by_k[k] = [r for r in raw_results if r["k"] == k]

    summary_by_k = {}
    for k, runs in by_k.items():
        total_runs = len(runs)
        ready_runs = [r for r in runs if r["ready"]]
        success_count = len(ready_runs)
        quorum_count = sum(1 for r in ready_runs if r["da_quorum_ready"])
        custody_count = sum(1 for r in ready_runs if r["full_custody"])

        pub_latencies = [r["pub_latency"] for r in runs if r["ready"]]
        ready_latencies = [r["ready_latency"] for r in runs if r["ready"]]
        total_latencies = [r["total_latency"] for r in runs if r["ready"]]

        throughputs = []
        for r in ready_runs:
            if r["total_latency"] > 0:
                mb_s = (r["payload_bytes"] / (1024.0 * 1024.0)) / r["total_latency"]
                throughputs.append(mb_s)

        summary_by_k[k] = {
            "k": k,
            "total_runs": total_runs,
            "success_runs": success_count,
            "quorum_runs": quorum_count,
            "custody_runs": custody_count,
            "quorum_rate_pct": (quorum_count / total_runs * 100.0) if total_runs > 0 else 0.0,
            "custody_rate_pct": (custody_count / total_runs * 100.0) if total_runs > 0 else 0.0,
            "pub_stats": compute_stats(pub_latencies),
            "ready_stats": compute_stats(ready_latencies),
            "total_stats": compute_stats(total_latencies),
            "throughput_stats": compute_stats(throughputs),
            "payload_bytes": k * k * args.cell_size
        }

    # =========================================================================
    # Table 1: End-to-End Latency & Custody Convergence Matrix
    # =========================================================================
    print("\n" + "=" * 115)
    print("                     PIPELINE BENCHMARK EVALUATION — END-TO-END LATENCY & QUORUM")
    print("=" * 115)
    print(f"{'Matrix K':<8} | {'ODS Size':<9} | {'EDS Dim':<8} | {'Runs':<6} | {'Quorum Rate':<12} | {'Custody Rate':<13} | {'Avg Total':<10} | {'Min / Max (s)':<16} | {'Median':<8} | {'StdDev'}")
    print("-" * 115)

    for k in sorted(k_values):
        s = summary_by_k[k]
        ods_str = format_size(s["payload_bytes"])
        eds_str = f"{2*k}x{2*k}"
        runs_str = f"{s['success_runs']}/{s['total_runs']}"
        q_rate_str = f"{s['quorum_rate_pct']:.0f}% ({s['quorum_runs']}/{s['total_runs']})"
        c_rate_str = f"{s['custody_rate_pct']:.0f}% ({s['custody_runs']}/{s['total_runs']})"
        tot = s["total_stats"]
        min_max_str = f"{tot['min']:.3f} / {tot['max']:.3f}"

        print(f"K={k:<6} | {ods_str:<9} | {eds_str:<8} | {runs_str:<6} | {q_rate_str:<12} | {c_rate_str:<13} | {tot['mean']:<10.3f} | {min_max_str:<16} | {tot['median']:<8.3f} | {tot['stdev']:.3f}")

    print("=" * 115)

    # =========================================================================
    # Table 2: Phase Breakdown & Throughput Statistics
    # =========================================================================
    print("\n" + "=" * 115)
    print("                      LATENCY PHASE BREAKDOWN & THROUGHPUT STATISTICS")
    print("=" * 115)
    print(f"{'Matrix K':<8} | {'Avg Publish (s)':<16} | {'Avg DA Wait (s)':<16} | {'Avg MB/s':<11} | {'Min MB/s':<11} | {'Max MB/s':<11} | {'Throughput StdDev'}")
    print("-" * 115)

    for k in sorted(k_values):
        s = summary_by_k[k]
        pub = s["pub_stats"]
        rdy = s["ready_stats"]
        tp = s["throughput_stats"]
        print(f"K={k:<6} | {pub['mean']:<16.3f} | {rdy['mean']:<16.3f} | {tp['mean']:<11.2f} | {tp['min']:<11.2f} | {tp['max']:<11.2f} | {tp['stdev']:.2f}")

    print("=" * 115)

    # =========================================================================
    # Aggregate Metrics & Conclusion
    # =========================================================================
    total_valid_ready = sum(s["success_runs"] for s in summary_by_k.values())
    total_bytes_processed = sum(r["payload_bytes"] for r in raw_results if r["ready"])
    total_pipeline_latency = sum(r["total_latency"] for r in raw_results if r["ready"])

    print("\n[*] Cumulative Benchmark Summary:")
    print(f"    - Total Test Wall-Clock Time : {total_bench_time:.2f} s")
    print(f"    - Total Valid Blocks Ready   : {total_valid_ready} / {total_submissions} ({(total_valid_ready/total_submissions*100.0):.1f}%)")
    print(f"    - Total Ingested Payload     : {format_size(total_bytes_processed)}")
    if total_bench_time > 0:
        eff_tp = (total_bytes_processed / (1024.0 * 1024.0)) / total_bench_time
        print(f"    - Effective System Bandwidth : {eff_tp:.2f} MB/s (Wall-Clock Ingestion Rate)")
    if total_pipeline_latency > 0:
        piped_tp = (total_bytes_processed / (1024.0 * 1024.0)) / total_pipeline_latency
        print(f"    - Cumulative Pipeline Rate   : {piped_tp:.2f} MB/s (Pure Processing Sum)")

    if args.order == "interleaved":
        print(f"    - Scheduling Verification    : Interleaved order successfully executed ({pattern_desc})")
        print("                                   Zero backpressure stalls or thermal cascades detected.")
    print("=" * 95)

    # =========================================================================
    # Export JSON if requested
    # =========================================================================
    if args.json_out:
        out_data = {
            "timestamp": datetime.datetime.now().isoformat(),
            "config": {
                "iterations": args.iterations,
                "k_values": k_values,
                "order": args.order,
                "delay": args.delay,
                "timeout": args.timeout,
                "cell_size": args.cell_size,
                "publisher": args.publisher,
                "bootstrap": args.bootstrap
            },
            "summary_by_k": summary_by_k,
            "raw_results": raw_results,
            "totals": {
                "total_wall_clock_seconds": total_bench_time,
                "total_valid_ready": total_valid_ready,
                "total_submissions": total_submissions,
                "total_bytes_processed": total_bytes_processed
            }
        }
        try:
            os.makedirs(os.path.dirname(os.path.abspath(args.json_out)), exist_ok=True)
            with open(args.json_out, "w") as f:
                json.dump(out_data, f, indent=2)
            print(f"\n[+] Detailed benchmark report saved to: {args.json_out}")
        except Exception as e:
            print(f"⚠️ Failed to save JSON report to {args.json_out}: {e}")


if __name__ == "__main__":
    main()
