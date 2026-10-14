#!/usr/bin/env python3
"""
cda_network_monitor.py - End-to-End Flow & Health Monitor for CDA Network.

Monitors the full multi-stage pipeline across all nodes:
  [STAGE 1: CONSENSUS]   CometBFT block commit & mempool
  [STAGE 2: PUBLISHER]   Publisher ODS matrix -> EDS expansion -> Kate Column Commitments
  [STAGE 3: STORAGE]     Store Nodes piece custody (Primary + Recoded pieces)
  [STAGE 4: BLOCK_READY] CDA Layer BlockReady event (via SSE / GossipSub) -> Light Nodes DAS
"""

import argparse
import concurrent.futures
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime

# ANSI Colors
CLR_RESET = "\033[0m"
CLR_BOLD = "\033[1m"
CLR_DIM = "\033[2m"
CLR_RED = "\033[31m"
CLR_GREEN = "\033[32m"
CLR_YELLOW = "\033[33m"
CLR_BLUE = "\033[34m"
CLR_MAGENTA = "\033[35m"
CLR_CYAN = "\033[36m"
CLR_WHITE = "\033[37m"
CLR_BG_BLUE = "\033[44m"


def http_get_json(url, timeout=2.0):
    try:
        req = urllib.request.Request(url, headers={'Accept': 'application/json', 'User-Agent': 'CDA-Monitor/1.0'})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status in (200, 204):
                body = resp.read().decode('utf-8')
                if not body.strip():
                    return None
                return json.loads(body)
            return None
    except Exception:
        return None


def parse_port_range(spec, default_host="localhost"):
    """
    Parses '9300-9307' or '9300:9307' or 'http://localhost:9300,http://localhost:9301' into a list of URLs.
    """
    urls = []
    if not spec:
        return urls
    parts = spec.split(",")
    for part in parts:
        part = part.strip()
        if not part:
            continue
        if part.startswith("http://") or part.startswith("https://"):
            urls.append(part)
        elif "-" in part or ":" in part:
            sep = "-" if "-" in part else ":"
            start_str, end_str = part.split(sep, 1)
            try:
                start_port = int(start_str)
                end_port = int(end_str)
                for p in range(start_port, end_port + 1):
                    urls.append(f"http://{default_host}:{p}")
            except ValueError:
                pass
        else:
            try:
                p = int(part)
                urls.append(f"http://{default_host}:{p}")
            except ValueError:
                pass
    return urls


class CDANetworkMonitor:
    def __init__(self, cometbft_url, publisher_url, bootstrap_url, store_urls, light_urls, mode="dashboard", interval=1.0):
        self.cometbft_url = cometbft_url.rstrip("/")
        self.publisher_url = publisher_url.rstrip("/")
        self.bootstrap_url = bootstrap_url.rstrip("/")
        self.store_urls = store_urls
        self.light_urls = light_urls
        self.mode = mode
        self.interval = interval

        self.lock = threading.Lock()
        self.stopped = False

        # State tracking
        self.history = {}  # height -> dict
        self.last_reported_stage = {}  # height -> highest stage reported in stream mode (1, 2, 3, 4)
        self.last_reported_ready_height = 0
        self.start_time = time.time()

        # Background SSE thread for instant BlockReady detection from Bootstrap
        self.sse_thread = threading.Thread(target=self._listen_sse, daemon=True)
        self.sse_thread.start()

    def _on_block_ready_event(self, payload):
        # payload: {"block_id": "block-15", "height": 15, "k": 16}
        b_id = payload.get("block_id", "")
        h = payload.get("height", 0)
        k = payload.get("k", 0)
        if h <= 0 and b_id.startswith("block-"):
            try:
                h = int(b_id.split("-")[-1])
            except ValueError:
                pass
        if h <= 0:
            return

        with self.lock:
            if h not in self.history:
                self.history[h] = {
                    "height": h,
                    "first_seen": time.time(),
                    "k": k,
                    "ods_cells": k * k if k else 0,
                    "eds_cells": 4 * k * k if k else 0,
                    "commits_root": "",
                    "store_completed": len(self.store_urls),
                    "store_total": len(self.store_urls),
                    "block_ready": True,
                    "ready_time": time.time(),
                }
            else:
                self.history[h]["block_ready"] = True
                if k > 0:
                    self.history[h]["k"] = k
                    self.history[h]["ods_cells"] = k * k
                    self.history[h]["eds_cells"] = 4 * k * k
                if "ready_time" not in self.history[h]:
                    self.history[h]["ready_time"] = time.time()

            if h > self.last_reported_ready_height:
                self.last_reported_ready_height = h

            if self.mode == "stream":
                self._emit_block_ready_stream(h)

    def _emit_block_ready_stream(self, h):
        if self.last_reported_stage.get(h, 0) >= 3:
            return
        entry = self.history.get(h, {})
        k = entry.get("k", 0)
        k_str = f"K={k} (ODS: {k}x{k}={k*k} cells, EDS: {2*k}x{2*k}={4*k*k} cells)" if k > 0 else "K=Auto"
        dur_str = ""
        if "first_seen" in entry and "ready_time" in entry:
            dur = entry["ready_time"] - entry["first_seen"]
            if dur >= 0.05:
                dur_str = f" | Latency: {dur:.2f}s"
        now_str = datetime.now().strftime("%H:%M:%S")
        print(f"[{now_str}] {CLR_GREEN}🎉 [STAGE 3: CDA BLOCK_READY]{CLR_RESET} Block #{h} ({f'block-{h}'}) reached CDA BlockReady | {k_str} | Store Nodes: 100%{dur_str} -> DAS Sampling Active!")
        self.last_reported_stage[h] = 3

    def _listen_sse(self):
        while not self.stopped:
            try:
                url = f"{self.bootstrap_url}/events/block-ready"
                req = urllib.request.Request(url, headers={'Accept': 'text/event-stream'})
                with urllib.request.urlopen(req, timeout=30) as resp:
                    for line in resp:
                        if self.stopped:
                            break
                        line_str = line.decode('utf-8').strip()
                        if line_str.startswith("data:"):
                            json_str = line_str[5:].strip()
                            if json_str:
                                try:
                                    payload = json.loads(json_str)
                                    self._on_block_ready_event(payload)
                                except Exception:
                                    pass
            except Exception:
                time.sleep(1.0)

    def query_cometbft(self):
        status_data = http_get_json(f"{self.cometbft_url}/status")
        mempool_data = http_get_json(f"{self.cometbft_url}/num_unconfirmed_txs")

        res = {
            "online": False,
            "moniker": "N/A",
            "chain_id": "N/A",
            "height": 0,
            "block_time": "N/A",
            "mempool_txs": 0,
            "mempool_bytes": 0,
        }
        if status_data and "result" in status_data:
            sync_info = status_data["result"].get("sync_info", {})
            node_info = status_data["result"].get("node_info", {})
            res["online"] = True
            res["moniker"] = node_info.get("moniker", "cometbft")
            res["chain_id"] = node_info.get("network", "cda-network")
            res["height"] = int(sync_info.get("latest_block_height", 0))
            raw_time = sync_info.get("latest_block_time", "")
            if raw_time:
                res["block_time"] = raw_time.split(".")[0].replace("T", " ")
        if mempool_data and "result" in mempool_data:
            res["mempool_txs"] = int(mempool_data["result"].get("n_txs", 0))
            res["mempool_bytes"] = int(mempool_data["result"].get("total_bytes", 0))
        return res

    def query_publisher(self):
        status_data = http_get_json(f"{self.publisher_url}/status")
        res = {
            "online": False,
            "in_flight": 0,
            "latest_published": 0,
            "latest_completed": 0,
            "active_cols": 8,
            "default_k": 32,
        }
        if status_data:
            res["online"] = True
            res["in_flight"] = status_data.get("in_flight_count", 0)
            res["latest_published"] = status_data.get("latest_published_height", 0)
            res["latest_completed"] = status_data.get("latest_completed_height", 0)
            res["active_cols"] = status_data.get("active_cols", 8)
            res["default_k"] = status_data.get("default_k", 32)
        else:
            health = http_get_json(f"{self.publisher_url}/health")
            if health:
                res["online"] = True
        return res

    def query_publisher_header(self, height):
        header_data = http_get_json(f"{self.publisher_url}/header/block-{height}")
        if header_data and "column_comm" in header_data:
            n = len(header_data.get("column_comm", []))
            k = n // 2
            return {
                "height": header_data.get("height", 0),
                "block_id": header_data.get("block_id", ""),
                "k": k,
                "n": n,
                "ods_cells": k * k,
                "eds_cells": n * n,
                "commits_root": header_data.get("commits_root", ""),
            }
        return None

    def query_bootstrap(self):
        health = http_get_json(f"{self.bootstrap_url}/health")
        peers_data = http_get_json(f"{self.bootstrap_url}/bootstrap/peers")
        ready_data = http_get_json(f"{self.bootstrap_url}/block-ready/latest")

        res = {
            "online": health is not None,
            "peer_count": 0,
            "latest_ready_block": "N/A",
            "latest_ready_height": 0,
            "latest_ready_k": 0,
        }
        if peers_data and "peers" in peers_data:
            res["peer_count"] = len(peers_data["peers"])
        if ready_data:
            res["latest_ready_block"] = ready_data.get("block_id", "N/A")
            res["latest_ready_height"] = ready_data.get("height", 0)
            res["latest_ready_k"] = ready_data.get("k", 0)
        return res

    def query_stores(self, height):
        target_block = f"block-{height}"
        results = []

        def check_store(idx, url):
            status = http_get_json(f"{url}/store/status/{target_block}", timeout=1.0)
            health = http_get_json(f"{url}/health", timeout=0.8)
            completed = False
            unmet = 0
            if status:
                completed = status.get("completed", False)
                details = status.get("details", {})
                if isinstance(details, dict):
                    unmet = details.get("unmet_count", 0)
            return {
                "idx": idx,
                "url": url,
                "online": health is not None or status is not None,
                "completed": completed,
                "unmet": unmet,
            }

        with concurrent.futures.ThreadPoolExecutor(max_workers=min(16, len(self.store_urls) or 1)) as ex:
            futs = [ex.submit(check_store, i, u) for i, u in enumerate(self.store_urls)]
            for fut in concurrent.futures.as_completed(futs):
                results.append(fut.result())

        results.sort(key=lambda x: x["idx"])
        online_count = sum(1 for r in results if r["online"])
        completed_count = sum(1 for r in results if r["completed"])
        return {
            "height": height,
            "stores": results,
            "total": len(self.store_urls),
            "online_count": online_count,
            "completed_count": completed_count,
            "all_completed": completed_count == len(self.store_urls) and len(self.store_urls) > 0,
        }

    def query_lights(self):
        online_count = 0
        for u in self.light_urls:
            h = http_get_json(f"{u}/health", timeout=0.8)
            if h is not None:
                online_count += 1
        return {
            "total": len(self.light_urls),
            "online_count": online_count,
        }

    def poll_all(self):
        c_status = self.query_cometbft()
        height = c_status["height"]
        p_status = self.query_publisher()
        b_status = self.query_bootstrap()
        l_status = self.query_lights()

        with self.lock:
            # 1. Register newly committed heights in history
            if height > 0:
                # Add heights up to current
                start_h = max(1, height - 20)
                for h_i in range(start_h, height + 1):
                    if h_i not in self.history:
                        self.history[h_i] = {
                            "height": h_i,
                            "first_seen": time.time(),
                            "k": 0,
                            "ods_cells": 0,
                            "eds_cells": 0,
                            "commits_root": "",
                            "store_completed": 0,
                            "store_total": len(self.store_urls),
                            "block_ready": False,
                        }

            # 2. Check Bootstrap / Publisher latest ready height
            ready_h = b_status.get("latest_ready_height", 0)
            if ready_h > 0 and ready_h in self.history:
                ent = self.history[ready_h]
                if not ent["block_ready"]:
                    ent["block_ready"] = True
                    if "ready_time" not in ent:
                        ent["ready_time"] = time.time()
                    if b_status.get("latest_ready_k", 0) > 0 and ent["k"] == 0:
                        ent["k"] = b_status["latest_ready_k"]
                        ent["ods_cells"] = b_status["latest_ready_k"] ** 2
                        ent["eds_cells"] = 4 * (b_status["latest_ready_k"] ** 2)

            # 3. For all in-flight blocks in history, resolve Publisher Header and Store custody
            active_heights = [h_i for h_i in sorted(self.history.keys()) if self.last_reported_stage.get(h_i, 0) < 3]
            # Take last 8 active heights
            for h_i in active_heights[-8:]:
                ent = self.history[h_i]
                # If missing header, query publisher
                if ent["k"] == 0:
                    hdr = self.query_publisher_header(h_i)
                    if hdr:
                        ent["k"] = hdr["k"]
                        ent["ods_cells"] = hdr["ods_cells"]
                        ent["eds_cells"] = hdr["eds_cells"]
                        ent["commits_root"] = hdr["commits_root"]

                # If store custody not verified, query stores
                if ent["store_completed"] < ent["store_total"] and not ent["block_ready"]:
                    s_res = self.query_stores(h_i)
                    ent["store_completed"] = s_res["completed_count"]
                elif ent["block_ready"]:
                    ent["store_completed"] = ent["store_total"]

        # Latest store status for the current height (for dashboard display)
        s_status = self.query_stores(height if height > 0 else 1)

        return c_status, p_status, b_status, s_status, l_status

    def render_stream(self, c_status, p_status, b_status, s_status, l_status):
        now_str = datetime.now().strftime("%H:%M:%S")

        with self.lock:
            # Iterate through all heights in history in chronological order
            sorted_heights = sorted(self.history.keys())
            for h in sorted_heights:
                entry = self.history[h]
                last_stage = self.last_reported_stage.get(h, 0)

                # Stage 1: Consensus Commit
                if last_stage < 1 and c_status["online"]:
                    print(f"[{now_str}] {CLR_CYAN}⚡ [STAGE 1: CONSENSUS]{CLR_RESET}   CometBFT committed block #{h} | Mempool: {c_status['mempool_txs']} txs ({c_status['mempool_bytes']} B) | Chain: {c_status['chain_id']}")
                    self.last_reported_stage[h] = 1

                # Stage 2: Publisher ODS / EDS & Commitments (chỉ in khi thực sự có Publisher header với commits_root)
                if last_stage < 2 and entry.get("k", 0) > 0 and entry.get("commits_root"):
                    k = entry["k"]
                    root = entry["commits_root"][:16] + "..." if entry["commits_root"] else "N/A"
                    print(f"[{now_str}] {CLR_YELLOW}📦 [STAGE 2: PUBLISHER]{CLR_RESET}   Block #{h} ODS processed: K={k} ({k}x{k} = {entry['ods_cells']} cells) -> EDS ({2*k}x{2*k} = {entry['eds_cells']} cells) | Kate Root: {root}")
                    self.last_reported_stage[h] = 2

                # Stage 3: CDA BlockReady Signal (Store Custody 100% -> DAS Active)
                if last_stage < 3 and entry.get("block_ready", False):
                    self._emit_block_ready_stream(h)

    def render_dashboard(self, c_status, p_status, b_status, s_status, l_status):
        # Clear screen and return cursor to home
        sys.stdout.write("\033[2J\033[H")

        term_width = 105
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print(f"{CLR_BOLD}{CLR_WHITE}{'=' * term_width}{CLR_RESET}")
        print(f"{CLR_BOLD}{CLR_CYAN}  CDA NETWORK - FULL PIPELINE END-TO-END FLOW MONITOR{CLR_RESET}   {CLR_DIM}[{now_str}]{CLR_RESET}")
        print(f"{CLR_BOLD}{CLR_WHITE}{'=' * term_width}{CLR_RESET}")

        # 1. Pipeline Flow Banner
        h = c_status["height"]
        with self.lock:
            entry = self.history.get(h, {})
            k_val = entry.get("k", p_status.get("default_k", 32))
            k_disp = f"K={k_val}" if k_val > 0 else "K=Auto"
            s_pct = (s_status["completed_count"] / max(1, s_status["total"])) * 100
            ready_icon = f"{CLR_GREEN}✅ READY{CLR_RESET}" if entry.get("block_ready", False) else f"{CLR_YELLOW}⏳ SYNCING{CLR_RESET}"

            print(f"\n{CLR_BOLD}  PIPELINE STAGES (Current Consensus Height #{h}):{CLR_RESET}")
            print(f"  [1. Mempool: {c_status['mempool_txs']} txs] ──> [2. CometBFT #{h}] ──> [3. Publisher {k_disp}] ──> [4. Bootstrap FK20] ──> [5. Store: {s_status['completed_count']}/{s_status['total']} ({s_pct:.0f}%)] ──> [6. CDA {ready_icon}]")

            # 2. Layer Status Cards
            print(f"\n{CLR_BOLD}  LAYER HEALTH & METRICS:{CLR_RESET}")
            c_badge = f"{CLR_GREEN}ONLINE{CLR_RESET}" if c_status["online"] else f"{CLR_RED}OFFLINE{CLR_RESET}"
            p_badge = f"{CLR_GREEN}ONLINE{CLR_RESET}" if p_status["online"] else f"{CLR_RED}OFFLINE{CLR_RESET}"
            b_badge = f"{CLR_GREEN}ONLINE{CLR_RESET}" if b_status["online"] else f"{CLR_RED}OFFLINE{CLR_RESET}"
            s_badge = f"{CLR_GREEN}{s_status['online_count']}/{s_status['total']} ONLINE{CLR_RESET}" if s_status["online_count"] == s_status["total"] else f"{CLR_YELLOW}{s_status['online_count']}/{s_status['total']} ONLINE{CLR_RESET}"
            l_badge = f"{CLR_GREEN}{l_status['online_count']}/{l_status['total']} ONLINE{CLR_RESET}"

            print(f"  ├─ {CLR_BOLD}Consensus Layer (CometBFT){CLR_RESET}   : [{c_badge}] Height: {CLR_BOLD}#{c_status['height']}{CLR_RESET} | Node: {c_status['moniker']} | Chain: {c_status['chain_id']}")
            print(f"  │  └─ Mempool                  : {c_status['mempool_txs']} unconfirmed txs ({c_status['mempool_bytes']} bytes) | Block Time: {c_status['block_time']}")
            print(f"  ├─ {CLR_BOLD}Publisher Node (8080){CLR_RESET}        : [{p_badge}] In-Flight: {p_status['in_flight']} | Active Cols: {p_status['active_cols']} | Completed H: #{p_status['latest_completed']}")
            print(f"  ├─ {CLR_BOLD}Bootstrap Relay (9200){CLR_RESET}       : [{b_badge}] Peers: {b_status['peer_count']} | Latest Ready: {CLR_BOLD}{b_status['latest_ready_block']}{CLR_RESET} (K={b_status['latest_ready_k']})")
            print(f"  ├─ {CLR_BOLD}Store Nodes Cluster{CLR_RESET}          : [{s_badge}] Ports {self.store_urls[0].split(':')[-1]}..{self.store_urls[-1].split(':')[-1]} (Custody for #{h}: {s_status['completed_count']}/{s_status['total']})")
            print(f"  └─ {CLR_BOLD}Light Nodes (DAS){CLR_RESET}            : [{l_badge}] Ports {self.light_urls[0].split(':')[-1] if self.light_urls else 'N/A'}..{self.light_urls[-1].split(':')[-1] if self.light_urls else 'N/A'}")

            # 3. Store Node Custody Matrix
            print(f"\n{CLR_BOLD}  STORE NODES CUSTODY MATRIX (Height #{h}):{CLR_RESET}")
            row_str = "  "
            for st in s_status["stores"]:
                port = st["url"].split(":")[-1]
                if st["completed"]:
                    row_str += f"[{port}: {CLR_GREEN}✓{CLR_RESET}] "
                elif st["online"]:
                    row_str += f"[{port}: {CLR_YELLOW}…{CLR_RESET}] "
                else:
                    row_str += f"[{port}: {CLR_RED}✗{CLR_RESET}] "
            print(row_str)

            # 4. Recent Committed Blocks History Table
            print(f"\n{CLR_BOLD}  RECENT COMMITTED BLOCKS HISTORY:{CLR_RESET}")
            print(f"  {'Height':<8} {'Block ID':<12} {'K':<6} {'ODS Matrix':<14} {'EDS Matrix':<14} {'Kate Root (Commits)':<22} {'Store Custody':<16} {'CDA BlockReady':<16}")
            print(f"  {'-' * 110}")

            sorted_heights = sorted(self.history.keys(), reverse=True)[:10]
            if not sorted_heights:
                print(f"  {CLR_DIM}Waiting for blocks to commit...{CLR_RESET}")
            else:
                for h_item in sorted_heights:
                    ent = self.history[h_item]
                    k = ent.get("k", 0)
                    k_str = str(k) if k > 0 else "-"
                    ods_str = f"{k}x{k} ({k*k}c)" if k > 0 else "-"
                    eds_str = f"{2*k}x{2*k} ({4*k*k}c)" if k > 0 else "-"
                    root_str = ent.get("commits_root", "")
                    root_disp = root_str[:16] + "..." if root_str else "-"
                    custody_str = f"{ent.get('store_completed', 0)}/{ent.get('store_total', len(self.store_urls))} Store"

                    if ent.get("block_ready", False):
                        dur_str = ""
                        if "first_seen" in ent and "ready_time" in ent:
                            dur = ent["ready_time"] - ent["first_seen"]
                            if dur >= 0.05:
                                dur_str = f" ({dur:.1f}s)"
                        status_str = f"{CLR_GREEN}✅ BlockReady{dur_str}{CLR_RESET}"
                    elif ent.get("store_completed", 0) == ent.get("store_total", len(self.store_urls)) and ent.get("store_total", 0) > 0:
                        status_str = f"{CLR_BLUE}💾 Custodied{CLR_RESET}"
                    else:
                        status_str = f"{CLR_YELLOW}⏳ Disseminating{CLR_RESET}"

                    print(f"  #{h_item:<7} {f'block-{h_item}':<12} {k_str:<6} {ods_str:<14} {eds_str:<14} {root_disp:<22} {custody_str:<16} {status_str:<16}")

        print(f"\n{CLR_BOLD}{CLR_WHITE}{'=' * term_width}{CLR_RESET}")
        print(f"{CLR_DIM}  Press Ctrl+C to exit monitor. Mode: {self.mode.upper()} | Poll interval: {self.interval}s | SSE: Active{CLR_RESET}")
        sys.stdout.flush()

    def run(self):
        print(f"Connecting to CDA Network components (CometBFT: {self.cometbft_url}, Publisher: {self.publisher_url}, Bootstrap: {self.bootstrap_url})...")
        try:
            while True:
                c_status, p_status, b_status, s_status, l_status = self.poll_all()
                if self.mode == "dashboard":
                    self.render_dashboard(c_status, p_status, b_status, s_status, l_status)
                else:
                    self.render_stream(c_status, p_status, b_status, s_status, l_status)
                time.sleep(self.interval)
        except KeyboardInterrupt:
            self.stopped = True
            print("\nMonitor stopped.")


def main():
    parser = argparse.ArgumentParser(description="CDA Network - Full Flow & Health Monitor")
    parser.add_argument("--cometbft", type=str, default="http://localhost:26657", help="CometBFT RPC endpoint")
    parser.add_argument("--publisher", type=str, default="http://localhost:8080", help="CDA Publisher API endpoint")
    parser.add_argument("--bootstrap", type=str, default="http://localhost:9200", help="CDA Bootstrap API endpoint")
    parser.add_argument("--stores", type=str, default="9300-9307", help="Store nodes port range or URLs (default: 9300-9307)")
    parser.add_argument("--lights", type=str, default="9400-9402", help="Light nodes port range or URLs (default: 9400-9402)")
    parser.add_argument("--mode", type=str, default="dashboard", choices=["dashboard", "stream"], help="Display mode ('dashboard' or 'stream')")
    parser.add_argument("--interval", type=float, default=1.0, help="Polling interval in seconds (default: 1.0)")
    args = parser.parse_args()

    store_urls = parse_port_range(args.stores)
    light_urls = parse_port_range(args.lights)

    monitor = CDANetworkMonitor(
        cometbft_url=args.cometbft,
        publisher_url=args.publisher,
        bootstrap_url=args.bootstrap,
        store_urls=store_urls,
        light_urls=light_urls,
        mode=args.mode,
        interval=args.interval
    )
    monitor.run()


if __name__ == '__main__':
    main()
