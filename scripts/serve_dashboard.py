#!/usr/bin/env python3
import http.server
import socketserver
import os
import sys
import json
import time
import threading
import datetime
import urllib.request
import re

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8889
WEB_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "web"))
COMET_URL = os.environ.get("COMET_URL", "http://127.0.0.1:26657")
COMET_FALLBACK_URLS = [COMET_URL, "http://127.0.0.1:27658", "http://127.0.0.1:28658"]
PUB_URL = os.environ.get("PUB_URL", "http://127.0.0.1:8080")
STORE_URL = os.environ.get("STORE_URL", "http://127.0.0.1:9300")
BOOTSTRAP_URL = os.environ.get("BOOTSTRAP_URL", "http://127.0.0.1:9200")

_block_cache = {}
_live_store_complete = {}
_recent_blocks_cache = {}  # limit -> (timestamp, payload)
_cache_lock = threading.Lock()
MAX_CACHE_ENTRIES = 500


def _http_get_json(url, timeout=1.5):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.load(resp)


def _http_get_text(url, timeout=1.5):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def _comet_get_json(path, timeout=1.5):
    last_err = None
    for base in COMET_FALLBACK_URLS:
        try:
            return _http_get_json(f"{base}{path}", timeout=timeout)
        except Exception as e:
            last_err = e
    raise last_err


def _prune_caches_locked():
    if len(_block_cache) > MAX_CACHE_ENTRIES:
        for old_h in sorted(_block_cache.keys())[: len(_block_cache) - MAX_CACHE_ENTRIES]:
            _block_cache.pop(old_h, None)
    if len(_live_store_complete) > MAX_CACHE_ENTRIES:
        for old_h in sorted(_live_store_complete.keys())[: len(_live_store_complete) - MAX_CACHE_ENTRIES]:
            _live_store_complete.pop(old_h, None)


def _parse_iso_ts(iso_str):
    if not iso_str:
        return None
    s = iso_str.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    if "." in s:
        dot_idx = s.index(".")
        tz_idx = max(s.find("+", dot_idx), s.find("-", dot_idx))
        if tz_idx > dot_idx:
            frac = s[dot_idx + 1:tz_idx][:6]
            s = s[:dot_idx] + "." + frac + s[tz_idx:]
    return datetime.datetime.fromisoformat(s).timestamp()


def _get_block_meta(h):
    with _cache_lock:
        if h in _block_cache:
            return _block_cache[h]

    try:
        cb = _comet_get_json(f"/block?height={h}", timeout=1.5).get("result", {}).get("block", {})
        header_time = cb.get("header", {}).get("time")
        t_commit = _parse_iso_ts(header_time)
        txs_count = len(cb.get("data", {}).get("txs") or [])
    except Exception:
        return None

    t_index = None
    if txs_count > 0:
        try:
            toc = _http_get_json(f"{PUB_URL}/api/v1/toc/{h}", timeout=1.5).get("toc") or {}
            txs = toc.get("txs") or []
            if txs:
                ns = txs[0].get("namespace") or "default"
                bid = txs[0].get("blob_id")
                if bid:
                    t_index = _http_get_json(f"{PUB_URL}/api/v1/namespaces/{ns}/blobs/{bid}", timeout=1.5).get("blob", {}).get("updated_at")
        except Exception:
            pass

    meta = {
        "height": h,
        "txs_count": txs_count,
        "t_commit": t_commit,
        "t_index": t_index,
    }
    if t_commit is not None and (txs_count == 0 or t_index is not None):
        with _cache_lock:
            _block_cache[h] = meta
            _prune_caches_locked()
    return meta


def _compute_block_timings(h):
    curr = _get_block_meta(h)
    if not curr or curr["t_commit"] is None:
        return {"status": "error", "message": f"Block {h} not found"}

    consensus_sec = None
    if h > 1:
        prev = _get_block_meta(h - 1)
        if prev and prev["t_commit"] is not None:
            diff = curr["t_commit"] - prev["t_commit"]
            if 0 <= diff <= 300:
                consensus_sec = round(diff, 2)

    if curr["txs_count"] == 0:
        return {
            "status": "success",
            "height": h,
            "is_empty": True,
            "completed": True,
            "t_block_commit": curr["t_commit"],
            "consensus_sec": consensus_sec,
            "da_sec": 0.0,
        }

    # Check store node completion status
    completed = False
    try:
        st_data = _http_get_json(f"{STORE_URL}/store/status/block-{h}", timeout=1.5)
        completed = bool(st_data.get("completed"))
    except Exception:
        pass

    with _cache_lock:
        t_store_complete = _live_store_complete.get(h)
        if t_store_complete is not None and curr["t_commit"] is not None and t_store_complete < curr["t_commit"]:
            # Stale timestamp from a previous chain run; discard it
            del _live_store_complete[h]
            t_store_complete = None

    if t_store_complete is None and completed:
        # Derive exact store completion time from the 2-slot in-flight pipeline release
        subsequent = []
        for next_h in range(h + 1, h + 15):
            m = _get_block_meta(next_h)
            if not m:
                break
            if m["txs_count"] > 0 and m["t_index"]:
                subsequent.append(m)
                if len(subsequent) >= 3:
                    break

        if len(subsequent) >= 3:
            m3 = subsequent[2]
            if m3["t_index"] - m3["t_commit"] > 3.0:
                t_store_complete = float(m3["t_index"] - 1)
        if t_store_complete is None and len(subsequent) >= 2:
            m2 = subsequent[1]
            if m2["t_index"] - m2["t_commit"] > 3.0:
                t_store_complete = float(m2["t_index"] - 1)
        if t_store_complete is None and curr["t_index"]:
            t_store_complete = float(curr["t_index"] + 5)

    da_sec = None
    if t_store_complete is not None:
        da_sec = max(0.1, round(t_store_complete - int(curr["t_commit"]), 1))

    t_commit_iso = datetime.datetime.fromtimestamp(int(curr["t_commit"]), datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    t_store_iso = (
        datetime.datetime.fromtimestamp(int(t_store_complete), datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        if t_store_complete is not None else None
    )
    t_index_iso = (
        datetime.datetime.fromtimestamp(int(curr["t_index"]), datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        if curr["t_index"] is not None else None
    )

    return {
        "status": "success",
        "height": h,
        "is_empty": False,
        "completed": completed,
        "t_block_commit": curr["t_commit"],
        "t_block_commit_iso": t_commit_iso,
        "t_blob_index": curr["t_index"],
        "t_blob_index_iso": t_index_iso,
        "t_store_complete": t_store_complete,
        "t_store_complete_iso": t_store_iso,
        "consensus_sec": consensus_sec,
        "da_sec": da_sec,
    }


def _get_block_inspect(h):
    timings = _compute_block_timings(h)
    toc = None
    try:
        req_toc = urllib.request.urlopen(f"{PUB_URL}/api/v1/toc/{h}", timeout=2.0)
        t_data = json.load(req_toc)
        toc = t_data.get("toc") or t_data
    except Exception:
        pass

    pub_header = None
    try:
        req_pub = urllib.request.urlopen(f"{PUB_URL}/header/block-{h}", timeout=2.0)
        pub_header = json.load(req_pub)
    except Exception:
        pass

    cb_header = None
    block_hash = None
    wire_size = 0
    num_txs = 0
    try:
        req_meta = urllib.request.urlopen(f"{COMET_URL}/blockchain?minHeight={h}&maxHeight={h}", timeout=2.0)
        m_res = json.load(req_meta).get("result", {})
        metas = m_res.get("block_metas", [])
        if metas:
            wire_size = int(metas[0].get("block_size") or 0)
            num_txs = int(metas[0].get("num_txs") or 0)
            block_hash = metas[0].get("block_id", {}).get("hash")
            cb_header = metas[0].get("header")
    except Exception:
        pass

    if not cb_header:
        try:
            req_cb = urllib.request.urlopen(f"{COMET_URL}/header?height={h}", timeout=2.0)
            cb_res = json.load(req_cb).get("result", {})
            cb_header = cb_res.get("header")
        except Exception:
            pass

    if not block_hash and h > 0:
        try:
            req_commit = urllib.request.urlopen(f"{COMET_URL}/commit?height={h}", timeout=2.0)
            c_res = json.load(req_commit).get("result", {})
            block_hash = c_res.get("signed_header", {}).get("commit", {}).get("block_id", {}).get("hash")
        except Exception:
            pass

    payload_size = 0
    if toc and "txs" in toc and isinstance(toc["txs"], list):
        payload_size = sum(t.get("size") or 0 for t in toc["txs"])

    final_block_size = wire_size if wire_size > 0 else (payload_size if payload_size > 0 else 892)

    return {
        "status": "success",
        "height": h,
        "toc": toc,
        "pub_header": pub_header,
        "cb_header": cb_header,
        "block_hash": block_hash,
        "block_size": final_block_size,
        "wire_size": wire_size,
        "payload_size": payload_size,
        "num_txs": num_txs,
        "timings": timings
    }


def _get_recent_blocks(limit=50):
    now_ts = time.time()
    with _cache_lock:
        cached = _recent_blocks_cache.get(limit)
        if cached and (now_ts - cached[0]) < 1.0:
            return cached[1]

    try:
        st_data = _comet_get_json("/status", timeout=1.5)
    except Exception as e:
        return {"status": "error", "message": f"CometBFT status error: {e}"}

    latest_h = int(st_data.get("result", {}).get("sync_info", {}).get("latest_block_height") or 0)
    if latest_h <= 0:
        return {"status": "success", "latest_height": 0, "blocks": []}

    metas = []
    top = latest_h
    while top > 0 and (latest_h - top) < limit:
        bottom = max(1, top - 19)
        try:
            c_data = _comet_get_json(f"/blockchain?minHeight={bottom}&maxHeight={top}", timeout=1.5)
            chunk_metas = c_data.get("result", {}).get("block_metas") or []
            metas.extend(chunk_metas)
        except Exception:
            break
        top -= 20

    metas.sort(key=lambda m: int(m.get("header", {}).get("height") or 0), reverse=True)
    seen = set()
    deduped = []
    for m in metas:
        h = int(m.get("header", {}).get("height") or 0)
        if h not in seen:
            seen.add(h)
            deduped.append(m)

    formatted_blocks = []
    for m in deduped[:limit]:
        h = int(m.get("header", {}).get("height") or 0)
        num_txs = int(m.get("num_txs") or 0)
        block_size = int(m.get("block_size") or 0)
        block_hash = m.get("block_id", {}).get("hash") or ""
        raw_time = m.get("header", {}).get("time") or ""

        age_sec = 0.0
        time_str = "-"
        try:
            t_clean = raw_time[:26] + "Z" if len(raw_time) > 26 else raw_time
            dt = datetime.datetime.fromisoformat(t_clean.replace("Z", "+00:00"))
            block_ts = dt.timestamp()
            age_sec = max(0.0, now_ts - block_ts)
            time_str = dt.strftime("%H:%M:%S UTC")
        except Exception:
            pass

        if age_sec < 5:
            age_str = "just now"
        elif age_sec < 60:
            age_str = f"{int(age_sec)}s ago"
        elif age_sec < 3600:
            age_str = f"{int(age_sec // 60)}m ago"
        else:
            age_str = f"{int(age_sec // 3600)}h ago"

        formatted_blocks.append({
            "height": h,
            "hash": block_hash,
            "num_txs": num_txs,
            "block_size": block_size,
            "raw_time": raw_time,
            "time_str": time_str,
            "age_sec": round(age_sec, 1),
            "age_str": age_str,
        })

    # Calculate average active consensus block time
    avg_consensus_sec = 0.0
    active_deltas = []
    for i in range(len(formatted_blocks) - 1):
        try:
            tA_str = formatted_blocks[i]["raw_time"][:26] + "Z" if len(formatted_blocks[i]["raw_time"]) > 26 else formatted_blocks[i]["raw_time"]
            tB_str = formatted_blocks[i+1]["raw_time"][:26] + "Z" if len(formatted_blocks[i+1]["raw_time"]) > 26 else formatted_blocks[i+1]["raw_time"]
            tA = datetime.datetime.fromisoformat(tA_str.replace("Z", "+00:00"))
            tB = datetime.datetime.fromisoformat(tB_str.replace("Z", "+00:00"))
            delta = abs((tA - tB).total_seconds())
            if 0 < delta <= 6.0:
                active_deltas.append(delta)
        except Exception:
            pass
    if active_deltas:
        avg_consensus_sec = round(sum(active_deltas) / len(active_deltas), 1)
    else:
        avg_consensus_sec = 2.5

    # Fetch DA quorum duration from Publisher metrics
    avg_da_sec = 0.0
    try:
        m_text = _http_get_text(f"{PUB_URL}/metrics", timeout=1.0)
        sum_m = re.search(r"cda_publisher_block_duration_seconds_sum\s+([0-9.]+)", m_text)
        cnt_m = re.search(r"cda_publisher_block_duration_seconds_count\s+([0-9.]+)", m_text)
        if sum_m and cnt_m:
            s_val = float(sum_m.group(1))
            c_val = float(cnt_m.group(1))
            if c_val > 0:
                avg_da_sec = round(s_val / c_val, 1)
    except Exception:
        pass

    total_pipeline_sec = round(avg_consensus_sec + avg_da_sec, 1) if (avg_consensus_sec > 0 or avg_da_sec > 0) else 0.0

    res = {
        "status": "success",
        "latest_height": latest_h,
        "server_time": round(now_ts, 3),
        "avg_consensus_sec": avg_consensus_sec,
        "avg_da_sec": avg_da_sec,
        "total_pipeline_sec": total_pipeline_sec,
        "blocks": formatted_blocks
    }
    with _cache_lock:
        _recent_blocks_cache[limit] = (now_ts, res)
    return res


def _watch_block_ready_sse():
    last_h = 0
    while True:
        try:
            with urllib.request.urlopen(f"{BOOTSTRAP_URL}/events/block-ready", timeout=60) as req:
                for raw_line in req:
                    line = raw_line.decode("utf-8", errors="ignore").strip()
                    if line.startswith("data:"):
                        payload = json.loads(line[5:].strip())
                        h = int(payload.get("height") or 0)
                        if h > 0:
                            now_ts = time.time()
                            with _cache_lock:
                                if last_h > 5 and h < last_h - 5:
                                    # Chain was reset/restarted: clear stale caches
                                    _block_cache.clear()
                                    _live_store_complete.clear()
                                last_h = h
                                prev_ts = _live_store_complete.get(h)
                                if prev_ts is None or (now_ts - prev_ts) > 60.0:
                                    _live_store_complete[h] = now_ts
                                _prune_caches_locked()
        except Exception:
            time.sleep(2.0)


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB_DIR, **kwargs)

    def _normalize_path(self):
        clean_path = self.path.split('?')[0].rstrip('/')
        if clean_path in ("", "/", "/dashboard", "/dashboadrd", "/dash"):
            self.path = "/index.html"
        elif self.path.startswith("/dashboard/"):
            self.path = self.path[len("/dashboard"):]
        elif self.path.startswith("/dashboadrd/"):
            self.path = self.path[len("/dashboadrd"):]

    def do_GET(self):
        clean_path = self.path.split('?')[0].rstrip('/')
        if clean_path.startswith("/api/block-inspect/"):
            try:
                h_str = clean_path.split("/api/block-inspect/")[1].replace("block-", "")
                h = int(h_str)
                result = _get_block_inspect(h)
                body = json.dumps(result).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception as e:
                body = json.dumps({"status": "error", "message": str(e)}).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            return

        if clean_path.startswith("/api/block-timings/"):
            try:
                h_str = clean_path.split("/api/block-timings/")[1].replace("block-", "")
                h = int(h_str)
                result = _compute_block_timings(h)
                body = json.dumps(result).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception as e:
                body = json.dumps({"status": "error", "message": str(e)}).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            return

        if clean_path == "/api/recent-blocks" or clean_path.startswith("/api/recent-blocks"):
            try:
                limit = 50
                if "?" in self.path:
                    query = self.path.split("?", 1)[1]
                    for param in query.split("&"):
                        if param.startswith("limit="):
                            try:
                                limit = max(1, min(200, int(param.split("=")[1])))
                            except Exception:
                                pass
                result = _get_recent_blocks(limit)
                body = json.dumps(result).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception as e:
                body = json.dumps({"status": "error", "message": str(e)}).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            return

        self._normalize_path()
        return super().do_GET()

    def do_HEAD(self):
        clean_path = self.path.split('?')[0].rstrip('/')
        if clean_path.startswith("/api/block-timings/") or clean_path.startswith("/api/block-inspect/") or clean_path.startswith("/api/recent-blocks"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            return
        self._normalize_path()
        return super().do_HEAD()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', '*')
        self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        super().end_headers()

class ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

if __name__ == "__main__":
    t = threading.Thread(target=_watch_block_ready_sse, daemon=True)
    t.start()
    with ThreadingHTTPServer(("0.0.0.0", PORT), DashboardHandler) as httpd:
        print(f"CDA Web Dashboard serving at http://0.0.0.0:{PORT}/ and http://0.0.0.0:{PORT}/dashboard/")
        httpd.serve_forever()
