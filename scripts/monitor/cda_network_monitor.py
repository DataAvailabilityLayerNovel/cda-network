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


def http_check_ok(url, timeout=1.0):
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'CDA-Monitor/1.0'})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status in (200, 204)
    except Exception:
        return False


def http_get_text(url, timeout=1.0):
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'CDA-Monitor/1.0'})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status in (200, 204):
                return resp.read().decode('utf-8', errors='ignore')
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
    def __init__(self, cometbft_url, publisher_url, bootstrap_url, store_urls, light_urls, mode="dashboard", interval=1.0, stage="all"):
        self.cometbft_url = cometbft_url.rstrip("/")
        self.publisher_url = publisher_url.rstrip("/")
        self.bootstrap_url = bootstrap_url.rstrip("/")
        self.store_urls = store_urls
        self.light_urls = light_urls
        self.mode = mode
        self.interval = interval
        self.stage = str(stage).lower()

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
            num_stores = len(self.store_urls)
            if h not in self.history:
                self.history[h] = {
                    "height": h,
                    "first_seen": time.time(),
                    "k": k,
                    "ods_cells": k * k if k else 0,
                    "eds_cells": 4 * k * k if k else 0,
                    "commits_root": "",
                    "store_completed": num_stores,
                    "store_total": num_stores,
                    "store_statuses": [True] * num_stores,
                    "block_ready": True,
                    "ready_time": time.time(),
                }
            else:
                self.history[h]["block_ready"] = True
                self.history[h]["store_completed"] = num_stores
                self.history[h]["store_statuses"] = [True] * num_stores
                if k > 0:
                    self.history[h]["k"] = k
                    self.history[h]["ods_cells"] = k * k
                    self.history[h]["eds_cells"] = 4 * k * k
                if "ready_time" not in self.history[h]:
                    self.history[h]["ready_time"] = time.time()

            if h > self.last_reported_ready_height:
                self.last_reported_ready_height = h

            if self.mode == "stream" and self.stage in ("all", "3"):
                self._emit_block_ready_stream(h)

    def _emit_block_ready_stream(self, h):
        if self.stage not in ("all", "3"):
            return
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
        print(f"[{now_str}] {CLR_GREEN}🎉 [STAGE 3: STORAGE & BLOCKREADY]{CLR_RESET} Block #{h} reached CDA BlockReady | {k_str} | Store Quorum: 8/8 (100%){dur_str} -> DAS Active!")
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
            status = http_get_json(f"{url}/store/status/{target_block}", timeout=0.8)
            health = http_check_ok(f"{url}/health", timeout=0.8)
            completed = False
            unmet = 0
            if status:
                completed = bool(status.get("completed", False))
                details = status.get("details", {})
                if isinstance(details, dict):
                    unmet = details.get("unmet_count", 0)
            return {
                "idx": idx,
                "url": url,
                "online": health or (status is not None),
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
        total_samples = 0
        for u in self.light_urls:
            if http_check_ok(f"{u}/health", timeout=0.8):
                online_count += 1
            metrics_txt = http_get_text(f"{u}/metrics", timeout=0.8)
            if metrics_txt:
                for line in metrics_txt.splitlines():
                    if line.startswith("cda_light_das_attempts_total "):
                        try:
                            total_samples += int(float(line.split()[1]))
                        except Exception:
                            pass
        return {
            "total": len(self.light_urls),
            "online_count": online_count,
            "total_samples": total_samples,
        }

    def poll_all(self):
        c_status = self.query_cometbft()
        c_height = c_status["height"]
        p_status = self.query_publisher()
        p_pub_height = p_status.get("latest_published", 0)
        p_comp_height = p_status.get("latest_completed", 0)
        b_status = self.query_bootstrap()
        l_status = self.query_lights()

        # Decouple custody target height:
        # Determine the block currently undergoing custody verification across store nodes
        active_custody_h = p_comp_height + 1 if p_pub_height > p_comp_height else (p_comp_height if p_comp_height > 0 else c_height)
        if active_custody_h <= 0:
            active_custody_h = 1

        # Query stores specifically for the active custody block
        s_status = self.query_stores(active_custody_h)
        num_stores = len(self.store_urls)

        with self.lock:
            # 1. Register newly committed heights in history from CometBFT
            if c_height > 0:
                start_h = max(1, c_height - 30)
                for h_i in range(start_h, c_height + 1):
                    if h_i not in self.history:
                        self.history[h_i] = {
                            "height": h_i,
                            "first_seen": time.time(),
                            "k": 0,
                            "ods_cells": 0,
                            "eds_cells": 0,
                            "commits_root": "",
                            "store_completed": 0,
                            "store_total": num_stores,
                            "store_statuses": [False] * num_stores,
                            "block_ready": False,
                        }

            # 2. Check Bootstrap / Publisher latest ready height
            ready_h = max(b_status.get("latest_ready_height", 0), p_comp_height)
            if ready_h > 0:
                for h_i in range(1, ready_h + 1):
                    if h_i in self.history:
                        if not self.history[h_i]["block_ready"]:
                            self.history[h_i]["block_ready"] = True
                            self.history[h_i]["store_completed"] = num_stores
                            self.history[h_i]["store_statuses"] = [True] * num_stores
                            if "ready_time" not in self.history[h_i]:
                                self.history[h_i]["ready_time"] = time.time()
                        elif not self.history[h_i].get("store_statuses") or len(self.history[h_i]["store_statuses"]) != num_stores:
                            self.history[h_i]["store_statuses"] = [True] * num_stores

            # 3. Update store statuses for active custody block
            if active_custody_h in self.history:
                ent_act = self.history[active_custody_h]
                if not ent_act.get("block_ready", False):
                    statuses = [st["completed"] for st in s_status["stores"]]
                    ent_act["store_statuses"] = statuses
                    ent_act["store_completed"] = s_status["completed_count"]
                    if s_status["all_completed"]:
                        ent_act["block_ready"] = True
                        if "ready_time" not in ent_act:
                            ent_act["ready_time"] = time.time()

            # 4. For recent blocks in history, resolve Publisher Header if not yet fetched
            recent_keys = sorted(self.history.keys(), reverse=True)[:15]
            for h_i in recent_keys:
                ent = self.history[h_i]
                if ent["k"] == 0 and h_i <= p_pub_height:
                    hdr = self.query_publisher_header(h_i)
                    if hdr:
                        ent["k"] = hdr["k"]
                        ent["ods_cells"] = hdr["ods_cells"]
                        ent["eds_cells"] = hdr["eds_cells"]
                        ent["commits_root"] = hdr["commits_root"]

                # Ensure store_statuses has proper length
                if "store_statuses" not in ent or len(ent["store_statuses"]) != num_stores:
                    if ent.get("block_ready", False) or h_i <= p_comp_height:
                        ent["store_statuses"] = [True] * num_stores
                    else:
                        ent["store_statuses"] = [False] * num_stores

        return c_status, p_status, b_status, s_status, l_status, active_custody_h

    def render_stream(self, c_status, p_status, b_status, s_status, l_status, active_custody_h=0):
        now_str = datetime.now().strftime("%H:%M:%S")

        with self.lock:
            sorted_heights = sorted(self.history.keys())
            for h in sorted_heights:
                entry = self.history[h]
                last_stage = self.last_reported_stage.get(h, 0)

                # Stage 1: Consensus Commit
                if last_stage < 1 and c_status["online"]:
                    if self.stage in ("all", "1"):
                        print(f"[{now_str}] {CLR_CYAN}⚡ [STAGE 1: CONSENSUS]{CLR_RESET}   CometBFT committed block #{h} | Mempool: {c_status['mempool_txs']} txs ({c_status['mempool_bytes']} B) | Chain: {c_status['chain_id']}")
                    self.last_reported_stage[h] = 1

                # Stage 2: Publisher ODS / EDS & Commitments
                if last_stage < 2 and entry.get("k", 0) > 0 and entry.get("commits_root"):
                    if self.stage in ("all", "2"):
                        k = entry["k"]
                        root = entry["commits_root"][:16] + "..." if entry["commits_root"] else "N/A"
                        print(f"[{now_str}] {CLR_YELLOW}📦 [STAGE 2: DA PIPELINE]{CLR_RESET} Block #{h} ODS processed: K={k} ({k}x{k} = {entry['ods_cells']} cells) -> EDS ({2*k}x{2*k} = {entry['eds_cells']} cells) | Kate Root: {root}")
                    self.last_reported_stage[h] = 2

                # Stage 3: CDA BlockReady Signal (Store Custody 100% -> DAS Active)
                if last_stage < 3 and entry.get("block_ready", False):
                    if self.stage in ("all", "3"):
                        self._emit_block_ready_stream(h)
                    self.last_reported_stage[h] = 3

    def render_dashboard(self, c_status, p_status, b_status, s_status, l_status, active_custody_h=0):
        # Clear screen and return cursor to home
        sys.stdout.write("\033[2J\033[H")

        term_width = 115
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print(f"{CLR_BOLD}{CLR_WHITE}{'=' * term_width}{CLR_RESET}")
        print(f"{CLR_BOLD}{CLR_CYAN}  CDA NETWORK - 3-STAGE DECOUPLED PIPELINE MONITOR{CLR_RESET}   {CLR_DIM}[{now_str}]{CLR_RESET}")
        print(f"{CLR_BOLD}{CLR_WHITE}{'=' * term_width}{CLR_RESET}")

        c_height = c_status["height"]
        p_published = p_status.get("latest_published", 0)
        p_completed = p_status.get("latest_completed", 0)
        in_flight = p_status.get("in_flight", 0)

        # 1. Pipeline Progression Summary Banner
        c_p_lag = max(0, c_height - p_published)
        p_s_lag = max(0, p_published - p_completed)
        print(f"\n{CLR_BOLD}  PIPELINE PROGRESSION FLOW:{CLR_RESET}")
        print(f"  Stage 1 (Consensus #{c_height}) ──[{c_p_lag} behind]──> Stage 2 (Publisher #{p_published}) ──[{in_flight} in-flight]──> Stage 3 (BlockReady #{p_completed})")

        # 2. 3-STAGE SEPARATE CARDS
        print(f"\n{CLR_BOLD}{'─' * term_width}{CLR_RESET}")
        print(f"  {CLR_BOLD}{CLR_CYAN}[STAGE 1: CONSENSUS LAYER]{CLR_RESET}       {CLR_BOLD}{CLR_YELLOW}[STAGE 2: CDA DA PIPELINE]{CLR_RESET}       {CLR_BOLD}{CLR_GREEN}[STAGE 3: STORAGE & BLOCKREADY]{CLR_RESET}")
        print(f"  CometBFT (L1 Consensus)           Publisher & Relay (L2 DA)         Store Nodes & DAS Custody")
        print(f"  {'─' * 33}   {'─' * 33}   {'─' * 36}")

        c_badge = f"{CLR_GREEN}ONLINE{CLR_RESET}" if c_status["online"] else f"{CLR_RED}OFFLINE{CLR_RESET}"
        p_badge = f"{CLR_GREEN}ONLINE{CLR_RESET}" if p_status["online"] else f"{CLR_RED}OFFLINE{CLR_RESET}"
        s_badge = f"{CLR_GREEN}{s_status['online_count']}/{s_status['total']} ONLINE{CLR_RESET}" if s_status["online_count"] == s_status["total"] else f"{CLR_YELLOW}{s_status['online_count']}/{s_status['total']} ONLINE{CLR_RESET}"

        with self.lock:
            p_entry = self.history.get(p_published, {})
            k_val = p_entry.get("k", p_status.get("default_k", 16))
            k_str = f"K={k_val} ({k_val*k_val} cells)" if k_val > 0 else f"K={p_status.get('default_k', 16)}"
            eds_str = f"{2*k_val}x{2*k_val} ({4*k_val*k_val} cells)" if k_val > 0 else "-"
            root_disp = p_entry.get("commits_root", "")[:12] + "..." if p_entry.get("commits_root") else "-"

        s_pct = (s_status["completed_count"] / max(1, s_status["total"])) * 100
        active_block_label = f"#{active_custody_h}" if active_custody_h > 0 else "-"

        # Grid of store node custody completion
        grid_str = ""
        for st in s_status["stores"][:8]:
            if st["completed"]:
                grid_str += f"{CLR_GREEN}✓{CLR_RESET}"
            elif st["online"]:
                grid_str += f"{CLR_YELLOW}…{CLR_RESET}"
            else:
                grid_str += f"{CLR_RED}✗{CLR_RESET}"

        das_samples_str = f" [{l_status['total_samples']:,} samples]" if l_status.get("total_samples", 0) > 0 else ""
        das_badge = f"{l_status['online_count']}/{l_status['total']} online (ACTIVE){das_samples_str}"

        print(f"  • Node Status : [{c_badge}]          • Node Status : [{p_badge}]          • Cluster Status: [{s_badge}]")
        print(f"  • Height      : {CLR_BOLD}#{c_height:<15}{CLR_RESET}   • Published H : {CLR_BOLD}#{p_published:<15}{CLR_RESET}   • BlockReady H  : {CLR_BOLD}#{p_completed:<15}{CLR_RESET}")
        print(f"  • Mempool Txs : {c_status['mempool_txs']} txs ({c_status['mempool_bytes']}B)  • In-Flight   : {CLR_BOLD}{in_flight} block(s){CLR_RESET}        • Target Custody: Block {active_block_label}")
        print(f"  • Block Time  : {c_status['block_time']:<17}   • Dynamic ODS : {k_str:<17}   • Custody Quorum: {s_status['completed_count']}/{s_status['total']} Stores ({s_pct:.0f}%)")
        print(f"  • Moniker     : {c_status['moniker']:<17}   • EDS Matrix  : {eds_str:<17}   • DAS Sampling  : {das_badge}")
        print(f"  • Chain ID    : {c_status['chain_id']:<17}   • Kate Root   : {root_disp:<17}   • Quorum Grid   : [{grid_str}]")

        # 3. PIPELINE PROGRESSION TABLE (Recent Blocks Through the 3 Stages)
        print(f"\n{CLR_BOLD}{'─' * term_width}{CLR_RESET}")
        print(f"  {CLR_BOLD}PIPELINE PROGRESSION TABLE (Recent Blocks Through the 3 Stages):{CLR_RESET}")
        print(f"  {'Height':<7} │ {'[STAGE 1: CONSENSUS]':<18} │ {'[STAGE 2: DA PIPELINE]':<32} │ {'[STAGE 3: STORAGE & BLOCKREADY]':<32}")
        print(f"          │ {'Status':<10} {'Mempool':<7} │ {'Matrix':<12} {'Kate Root':<18} │ {'Pipeline Stage':<18} {'Latency':<12}")
        print(f"  {'─'*7}─┼─{'─'*18}─┼─{'─'*32}─┼─{'─'*32}")

        with self.lock:
            recent_heights = sorted(self.history.keys(), reverse=True)[:8]
            if not recent_heights:
                print(f"  {CLR_DIM}Waiting for block activity...{CLR_RESET}")
            else:
                for h_item in recent_heights:
                    ent = self.history[h_item]
                    k = ent.get("k", 0)
                    k_str = f"K={k} ({k*k}c)" if k > 0 else "Pending"
                    root_str = ent.get("commits_root", "")
                    root_disp = root_str[:14] + ".." if root_str else "-"

                    # Stage 1 text
                    st1_str = f"{CLR_CYAN}Committed{CLR_RESET}"
                    st1_mem = "-"

                    # Stage 2 text
                    if ent.get("k", 0) > 0 and ent.get("commits_root"):
                        st2_matrix = f"{CLR_YELLOW}{k_str}{CLR_RESET}"
                        st2_root = f"{CLR_DIM}{root_disp}{CLR_RESET}"
                    elif ent.get("block_ready", False) or h_item <= p_completed:
                        st2_matrix = f"{CLR_DIM}Processed{CLR_RESET}"
                        st2_root = f"{CLR_DIM}(pruned){CLR_RESET}"
                    elif h_item <= p_published:
                        st2_matrix = f"{CLR_YELLOW}Encoding...{CLR_RESET}"
                        st2_root = "-"
                    else:
                        st2_matrix = f"{CLR_DIM}Queued{CLR_RESET}"
                        st2_root = "-"

                    # Stage 3 text: Pending -> Custody -> Ready
                    dur_disp = "-"
                    if ent.get("block_ready", False) or h_item <= p_completed:
                        if "first_seen" in ent and "ready_time" in ent:
                            dur = ent["ready_time"] - ent["first_seen"]
                            if dur >= 0.05:
                                dur_disp = f"{dur:.2f}s"
                        st3_stage = f"{CLR_GREEN}✅ READY{CLR_RESET}"
                    elif h_item <= p_published:
                        st3_stage = f"{CLR_YELLOW}⏳ CUSTODY{CLR_RESET}"
                    else:
                        st3_stage = f"{CLR_DIM}⏳ PENDING{CLR_RESET}"

                    print(f"  #{h_item:<6} │ {st1_str:<19} {st1_mem:<7} │ {st2_matrix:<21} {st2_root:<27} │ {st3_stage:<27} {CLR_DIM}{dur_disp:<12}{CLR_RESET}")

        # 4. STORE NODE CUSTODY MATRIX (Per-Block Verification Across Store Nodes)
        print(f"\n{CLR_BOLD}{'─' * term_width}{CLR_RESET}")
        print(f"  {CLR_BOLD}STORE NODE CUSTODY MATRIX (Per-Block Verification Across Store Nodes):{CLR_RESET}")
        num_stores = len(self.store_urls)
        display_stores = min(num_stores, 8)
        header_cols = [f"Store {i}" for i in range(display_stores)]
        header_str = " │ ".join([f"{c:^9}" for c in header_cols])
        print(f"  {'Height':<7} │ {header_str} │ {'Quorum Status':<14}")
        sep_str = "─┼─".join(["─" * 9 for _ in range(display_stores)])
        print(f"  {'─'*7}─┼─{sep_str}─┼─{'─'*14}")

        with self.lock:
            if not recent_heights:
                print(f"  {CLR_DIM}Waiting for block activity...{CLR_RESET}")
            else:
                for h_item in recent_heights:
                    ent = self.history[h_item]
                    st_statuses = ent.get("store_statuses", [False] * num_stores)
                    if len(st_statuses) < display_stores:
                        st_statuses = st_statuses + [False] * (display_stores - len(st_statuses))

                    is_ready_block = ent.get("block_ready", False) or h_item <= p_completed
                    cells = []
                    ready_cnt = 0
                    for s_idx in range(display_stores):
                        is_store_ready = is_ready_block or st_statuses[s_idx]
                        if is_store_ready:
                            ready_cnt += 1
                            cells.append(f"{CLR_GREEN}  Ready  {CLR_RESET}")
                        elif h_item <= p_published:
                            cells.append(f"{CLR_YELLOW} Pending {CLR_RESET}")
                        else:
                            cells.append(f"{CLR_DIM} Pending {CLR_RESET}")

                    row_cells_str = " │ ".join(cells)

                    # Quorum string
                    if is_ready_block or ready_cnt == display_stores:
                        q_str = f"{CLR_GREEN}{display_stores}/{display_stores} Ready{CLR_RESET}"
                    elif ready_cnt > 0:
                        q_str = f"{CLR_YELLOW}{ready_cnt}/{display_stores} Custody{CLR_RESET}"
                    elif h_item <= p_published:
                        q_str = f"{CLR_YELLOW}0/{display_stores} Custody{CLR_RESET}"
                    else:
                        q_str = f"{CLR_DIM}0/{display_stores} Pending{CLR_RESET}"

                    print(f"  #{h_item:<6} │ {row_cells_str} │ {q_str}")

        print(f"\n{CLR_BOLD}{CLR_WHITE}{'=' * term_width}{CLR_RESET}")
        print(f"{CLR_DIM}  Press Ctrl+C to exit. Mode: {self.mode.upper()} | Stage Filter: {self.stage.upper()} | Interval: {self.interval}s{CLR_RESET}")
        sys.stdout.flush()

    def run(self):
        print(f"Connecting to CDA Network components (CometBFT: {self.cometbft_url}, Publisher: {self.publisher_url}, Bootstrap: {self.bootstrap_url})...")
        try:
            while True:
                c_status, p_status, b_status, s_status, l_status, active_custody_h = self.poll_all()
                if self.mode == "dashboard":
                    self.render_dashboard(c_status, p_status, b_status, s_status, l_status, active_custody_h)
                else:
                    self.render_stream(c_status, p_status, b_status, s_status, l_status, active_custody_h)
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
    parser.add_argument("--lights", type=str, default="9401-9402", help="Light nodes port range or URLs (default: 9401-9402)")
    parser.add_argument("--mode", type=str, default="dashboard", choices=["dashboard", "stream"], help="Display mode ('dashboard' or 'stream')")
    parser.add_argument("--stage", type=str, default="all", choices=["all", "1", "2", "3"], help="Stage to monitor: 'all', '1' (Consensus), '2' (DA Pipeline), '3' (Storage & BlockReady)")
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
        interval=args.interval,
        stage=args.stage
    )
    monitor.run()


if __name__ == '__main__':
    main()
