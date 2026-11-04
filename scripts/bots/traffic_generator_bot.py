#!/usr/bin/env python3
"""
traffic_generator_bot.py - Continuous Multi-Account Transaction Emitter for CDA Network.

Design Principles:
1. Pure Client Emitting: Bots submit transactions independently into the CometBFT mempool.
   No coupling or waiting for consensus block production. CometBFT proposer packs transactions
   naturally according to its own schedule and ODS matrix bounds.
2. Parallel Accounts: Multiple autonomous worker threads (bot-1, bot-2, ..., bot-N),
   desynchronized with micro-jitter so transactions stream smoothly into the mempool.
3. Workload Diversity: Each submission round randomly selects or alternates between 1, 3, 5, or 7
   transactions, with randomized payload sizes (e.g. 512B - 32KB) per transaction.
4. Clean URI Protocol: Each transaction is an independent entity formatted as:
   cda/<account>/tx_<uuid>=<hex_payload>
5. Non-blocking Ingestion: Submits via broadcast_tx_sync or broadcast_tx_async without locking.
"""

import argparse
import base64
import json
import os
import random
import signal
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def http_get_json(url, timeout=5):
    req = urllib.request.Request(url, headers={'Accept': 'application/json'})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode('utf-8'))


def submit_tx_to_cometbft(cometbft_rpc, tx_bytes, method="broadcast_tx_sync", timeout=15):
    if isinstance(tx_bytes, str):
        tx_bytes = tx_bytes.encode('utf-8')
    b64_tx = base64.b64encode(tx_bytes).decode('ascii')
    payload = {
        "jsonrpc": "2.0",
        "id": int(time.time() * 1000) % 1000000,
        "method": method,
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
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        res = json.loads(resp.read().decode('utf-8'))
        if "error" in res:
            raise RuntimeError(f"CometBFT RPC Error: {res['error']}")
        return res


class TrafficGenerator:
    def __init__(self, args):
        self.args = args
        self.stop_event = threading.Event()
        self.accounts = [f"{args.account_prefix}-{i+1}" for i in range(args.accounts)]

        # Support both --tx-counts and legacy --batch-sizes
        raw_counts = args.tx_counts or args.batch_sizes or "1,3,5,7"
        self.tx_counts = [int(x.strip()) for x in raw_counts.split(",") if x.strip()]
        if not self.tx_counts:
            self.tx_counts = [1, 3, 5, 7]

        self.method = "broadcast_tx_async" if args.method == "async" else "broadcast_tx_sync"

        # Stats
        self.stats_lock = threading.Lock()
        self.total_txs_submitted = 0
        self.total_bytes_submitted = 0
        self.account_txs = {acc: 0 for acc in self.accounts}
        self.account_rounds = {acc: 0 for acc in self.accounts}
        self.start_time = None

    def worker_loop(self, account, worker_idx):
        # Desynchronize startup across bots to prevent artificial burst alignment
        time.sleep(random.uniform(0.05, 0.25) * worker_idx)
        round_idx = 0

        while not self.stop_event.is_set():
            round_idx += 1
            if self.args.rounds_per_account > 0 and round_idx > self.args.rounds_per_account:
                break

            # Determine number of txs to emit in this round
            if self.args.pattern in ("alternate", "cycle", "round-robin"):
                count = self.tx_counts[(round_idx - 1) % len(self.tx_counts)]
            else: # "random"
                count = random.choice(self.tx_counts)

            round_bytes = 0
            tx_sizes_desc = []
            t_start = time.time()

            for _ in range(count):
                if self.stop_event.is_set():
                    break

                payload_size = random.randint(self.args.min_payload, self.args.max_payload)
                payload_bytes = os.urandom(payload_size)
                tx_id = uuid.uuid4().hex[:12]
                key = f"cda/{account}/tx_{tx_id}"
                tx_str = f"{key}={payload_bytes.hex()}"
                tx_encoded = tx_str.encode('utf-8')

                try:
                    res = submit_tx_to_cometbft(self.args.cometbft, tx_encoded, method=self.method, timeout=10)
                    tx_res = res.get("result", {})
                    code = tx_res.get("code", 0)
                    if code != 0:
                        log_msg = str(tx_res.get('log', ''))
                        if "mempool is full" in log_msg.lower():
                            time.sleep(random.uniform(0.1, 0.3))
                            continue

                    round_bytes += len(tx_encoded)
                    tx_sizes_desc.append(f"{payload_size / 1024:.1f}KB")

                    with self.stats_lock:
                        self.total_txs_submitted += 1
                        self.total_bytes_submitted += len(tx_encoded)
                        self.account_txs[account] += 1

                except Exception as e:
                    time.sleep(0.3)
                    break

            dur_ms = (time.time() - t_start) * 1000

            with self.stats_lock:
                self.account_rounds[account] += 1

            if round_bytes > 0 and not self.args.quiet:
                sizes_info = ", ".join(tx_sizes_desc) if count <= 3 else f"avg {round_bytes/(count*1024):.1f}KB/tx"
                print(f"  [{account}] ➔ Submitted {count} txs ({round_bytes / 1024:.1f} KB in {dur_ms:.0f}ms) [{sizes_info}]")

            if self.args.total_txs > 0:
                with self.stats_lock:
                    if self.total_txs_submitted >= self.args.total_txs:
                        self.stop_event.set()
                        break

            # Sleep interval before this bot's next submission
            if not self.stop_event.is_set():
                if self.args.interval is not None and self.args.interval >= 0:
                    # Apply small natural jitter (+- 15%) so bots remain desynchronized
                    jitter = random.uniform(-0.15, 0.15) * self.args.interval
                    delay = max(0.01, self.args.interval + jitter)
                else:
                    delay = random.uniform(self.args.min_jitter, self.args.max_jitter)
                if delay > 0:
                    time.sleep(delay)

    def stats_reporter_loop(self):
        last_count = 0
        last_time = time.time()

        while not self.stop_event.is_set():
            time.sleep(self.args.status_interval)
            if self.stop_event.is_set():
                break

            now = time.time()
            elapsed_interval = now - last_time
            with self.stats_lock:
                total_txs = self.total_txs_submitted
                total_bytes = self.total_bytes_submitted

            interval_txs = total_txs - last_count
            current_rate = interval_txs / elapsed_interval if elapsed_interval > 0 else 0
            overall_elapsed = now - self.start_time
            overall_rate = total_txs / overall_elapsed if overall_elapsed > 0 else 0

            print(f"  ⚡ [Live Ingestion] Total: {total_txs:5d} txs ({total_bytes/1024:.1f} KB) | "
                  f"Instant: {current_rate:4.1f} tx/s | Avg: {overall_rate:4.1f} tx/s")

            last_count = total_txs
            last_time = now

    def run(self):
        self.start_time = time.time()
        print("=" * 80)
        print("🚀 CDA NETWORK - CONTINUOUS ASYNCHRONOUS TRANSACTION EMITTER")
        print(f"CometBFT RPC     : {self.args.cometbft} (Method: {self.method})")
        print(f"Parallel Accounts: {len(self.accounts)} ({', '.join(self.accounts)})")
        print(f"Submission Sizes : {self.tx_counts} txs per round (Pattern: {self.args.pattern})")
        print(f"Payload Range    : {self.args.min_payload} - {self.args.max_payload} bytes per transaction")
        delay_desc = f"{self.args.interval}s" if self.args.interval is not None else f"{self.args.min_jitter}s - {self.args.max_jitter}s (jitter)"
        print(f"Bot Delay/Jitter : {delay_desc}")
        print(f"Target Txs       : {'Continuous (Infinite)' if self.args.total_txs == 0 else self.args.total_txs}")
        print("=" * 80)

        # Health check CometBFT
        try:
            status = http_get_json(f"{self.args.cometbft}/status", timeout=5)
            sync_info = status.get("result", {}).get("sync_info", {})
            print(f"✅ CometBFT Connected | Current Height: #{sync_info.get('latest_block_height')}\n")
        except Exception as e:
            print(f"❌ Cannot connect to CometBFT at {self.args.cometbft}: {e}")
            sys.exit(1)

        # Start Periodic Reporter Thread
        if self.args.status_interval > 0:
            reporter = threading.Thread(target=self.stats_reporter_loop, daemon=True)
            reporter.start()

        # Start Account Worker Threads
        threads = []
        for idx, acc in enumerate(self.accounts):
            t = threading.Thread(target=self.worker_loop, args=(acc, idx), daemon=True)
            threads.append(t)
            t.start()

        # Handle clean Ctrl+C
        try:
            while not self.stop_event.is_set():
                time.sleep(0.5)
                if self.args.total_txs > 0:
                    with self.stats_lock:
                        if self.total_txs_submitted >= self.args.total_txs:
                            break
                if self.args.rounds_per_account > 0 and not any(t.is_alive() for t in threads):
                    break
        except KeyboardInterrupt:
            print("\n\n🛑 Stop signal received. Shutting down worker threads...")
        finally:
            self.stop_event.set()

        for t in threads:
            t.join(timeout=1.5)

        self.print_summary()

    def print_summary(self):
        elapsed = time.time() - self.start_time
        print("\n" + "=" * 80)
        print("📊 TRANSACTION EMITTER SUMMARY")
        print(f"Total Runtime         : {elapsed:.2f}s")
        print(f"Total Txs Submitted   : {self.total_txs_submitted} txs")
        print(f"Total Data Ingested   : {self.total_bytes_submitted / 1024:.2f} KB ({self.total_bytes_submitted / (1024*1024):.2f} MB)")
        if elapsed > 0:
            print(f"Average Ingestion Rate: {self.total_txs_submitted / elapsed:.1f} tx/s | {self.total_bytes_submitted / (1024 * elapsed):.1f} KB/s")

        print("\nAccount Breakdown:")
        for acc in self.accounts:
            tx_c = self.account_txs.get(acc, 0)
            r_c = self.account_rounds.get(acc, 0)
            print(f"  • {acc:10s}: {tx_c:5d} txs across {r_c:4d} submit rounds")
        print("=" * 80)


def main():
    parser = argparse.ArgumentParser(description="CDA Network - Multi-Account Continuous Transaction Emitter")
    parser.add_argument("--cometbft", type=str, default="http://localhost:26657", help="CometBFT RPC endpoint")
    parser.add_argument("--accounts", type=int, default=4, help="Number of concurrent bot accounts (default: 4)")
    parser.add_argument("--account-prefix", type=str, default="bot", help="Prefix for account namespaces (default: 'bot' -> bot-1, bot-2, ...)")
    parser.add_argument("--tx-counts", "--batch-sizes", type=str, default="1,3,5,7",
                        help="Comma-separated counts of txs to emit per round (default: '1,3,5,7')")
    parser.add_argument("--pattern", type=str, choices=["random", "alternate", "cycle", "round-robin"], default="random",
                        help="Pattern to pick count per round: 'random' or 'alternate'/'cycle' (default: 'random')")
    parser.add_argument("--min-payload", type=int, default=512, help="Minimum payload size in bytes per tx (default: 512)")
    parser.add_argument("--max-payload", type=int, default=32768, help="Maximum payload size in bytes per tx (default: 32768 = 32KB)")
    parser.add_argument("--interval", "--delay", type=float, default=None,
                        help="Delay in seconds between submit rounds for each bot (e.g. --delay 0.5 or --delay 1.0). Overrides jitter if set.")
    parser.add_argument("--min-jitter", type=float, default=0.2, help="Minimum sleep jitter in seconds per bot (default: 0.2, used when --delay is not set)")
    parser.add_argument("--max-jitter", type=float, default=0.8, help="Maximum sleep jitter in seconds per bot (default: 0.8, used when --delay is not set)")
    parser.add_argument("--method", type=str, choices=["sync", "async"], default="sync",
                        help="CometBFT broadcast method: 'sync' (broadcast_tx_sync) or 'async' (broadcast_tx_async, ultra-fast non-blocking)")
    parser.add_argument("--total-txs", type=int, default=0, help="Total transactions to submit across all accounts before stopping (0 = continuous, default: 0)")
    parser.add_argument("--rounds-per-account", type=int, default=0, help="Max submit rounds per account (0 = continuous, default: 0)")
    parser.add_argument("--status-interval", type=float, default=5.0, help="Interval in seconds for printing aggregate live ingestion rate (0 = disabled, default: 5.0)")
    parser.add_argument("--quiet", action="store_true", help="Quiet mode: suppress per-submit console lines, only print live stats")

    args = parser.parse_args()
    bot = TrafficGenerator(args)
    bot.run()


if __name__ == '__main__':
    main()
