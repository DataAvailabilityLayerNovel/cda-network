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
import subprocess
import signal
import shutil
import socket
import re

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8889
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
WEB_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "web"))
COMET_URL = os.environ.get("COMET_URL", "http://127.0.0.1:26657")
COMET_FALLBACK_URLS = [COMET_URL, "http://127.0.0.1:27658", "http://127.0.0.1:28658"]
PUB_URL = os.environ.get("PUB_URL", "http://127.0.0.1:8080")
STORE_URL = os.environ.get("STORE_URL", "http://127.0.0.1:9300")
BOOTSTRAP_URL = os.environ.get("BOOTSTRAP_URL", "http://127.0.0.1:9200")
LIGHT_URL = os.environ.get("LIGHT_URL", "http://127.0.0.1:9401")

_block_cache = {}
_live_store_complete = {}
_recent_blocks_cache = {}  # limit -> (timestamp, payload)
_cache_lock = threading.Lock()
_nodes_lock = threading.Lock()
_spawned_store_nodes = {}  # port -> {"proc": Popen, "row": int, "col": int, "started_at": float, "log_path": str}
_spawned_light_nodes = {}  # port -> {"proc": Popen, "started_at": float, "log_path": str}
_rep_lock = threading.Lock()
_reputation_records = {}   # peer_id -> dict(peer_id, row, col, trust_score, trust_tier, blocks_completed, blocks_missed, das_served_count, soft_fault_count, byzantine_strikes, straggler_forgiven, last_event)
_rep_block_completed = {}  # block_id -> set(peer_id)
_rep_block_missed = {}     # block_id -> set(peer_id)
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


def _get_avg_da_duration_sec():
    try:
        m_text = _http_get_text(f"{PUB_URL}/metrics", timeout=1.0)
        sum_m = re.search(r"cda_publisher_block_duration_seconds_sum\s+([0-9.]+)", m_text)
        cnt_m = re.search(r"cda_publisher_block_duration_seconds_count\s+([0-9.]+)", m_text)
        if sum_m and cnt_m:
            s_val = float(sum_m.group(1))
            c_val = float(cnt_m.group(1))
            if c_val > 0:
                return round(s_val / c_val, 2)
    except Exception:
        pass
    return 0.2


def _compute_block_timings(h):
    curr = _get_block_meta(h)
    if not curr or curr["t_commit"] is None:
        return {"status": "error", "message": f"Block {h} not found"}

    consensus_sec = None
    if h > 1:
        prev = _get_block_meta(h - 1)
        if prev and prev["t_commit"] is not None:
            diff = curr["t_commit"] - prev["t_commit"]
            if 0 < diff <= 10.0:
                consensus_sec = round(diff, 2)
            elif diff > 10.0:
                # When create_empty_blocks=false, header.time of height H inherits the pre-idle
                # LastCommit BFT time. Active round consensus execution takes ~timeout_commit (2.5s).
                consensus_sec = 2.5

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

    # Check store node completion status & Publisher exact DA duration
    completed = False
    try:
        st_data = _http_get_json(f"{STORE_URL}/store/status/block-{h}", timeout=1.5)
        completed = bool(st_data.get("completed"))
    except Exception:
        pass

    exact_da_sec = None
    try:
        pub_hdr = _http_get_json(f"{PUB_URL}/header/block-{h}", timeout=1.5)
        if pub_hdr and isinstance(pub_hdr.get("da_duration_sec"), (int, float)) and pub_hdr["da_duration_sec"] > 0:
            exact_da_sec = round(float(pub_hdr["da_duration_sec"]), 2)
            completed = True
    except Exception:
        pass

    with _cache_lock:
        t_store_complete = _live_store_complete.get(h)
        if t_store_complete is not None and curr["t_commit"] is not None and t_store_complete < curr["t_commit"]:
            # Stale timestamp from a previous chain run; discard it
            del _live_store_complete[h]
            t_store_complete = None

    da_sec = exact_da_sec
    if da_sec is None:
        if t_store_complete is not None and curr["t_commit"] is not None:
            raw_da = t_store_complete - float(curr["t_commit"])
            if 0 < raw_da <= 15.0:
                da_sec = round(raw_da, 2)
        if da_sec is None and completed:
            da_sec = _get_avg_da_duration_sec()

    # Anchor display timestamps accurately even when BFT header.time had an idle gap
    effective_commit_ts = float(curr["t_commit"])
    if curr["t_index"] and (curr["t_index"] - effective_commit_ts) > 15.0:
        effective_commit_ts = float(curr["t_index"]) - (da_sec or 0.2)
    if t_store_complete is None and da_sec is not None:
        t_store_complete = effective_commit_ts + da_sec

    t_commit_iso = datetime.datetime.fromtimestamp(int(effective_commit_ts), datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
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


def _is_port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.2)
        return s.connect_ex(("127.0.0.1", port)) == 0


def _fnv1a_64_custody_offset(block_id, target_net_row, sub_row_idx):
    if sub_row_idx < 0:
        sub_row_idx = 0
    if not block_id:
        return sub_row_idx
    h = 14695981039346656037
    prime = 1099511628211
    mask = (1 << 64) - 1
    for b in block_id.encode("utf-8"):
        h ^= b
        h = (h * prime) & mask
    for b in int(target_net_row).to_bytes(4, byteorder="little", signed=False):
        h ^= b
        h = (h * prime) & mask
    base_offset = h % 1048576
    return base_offset + sub_row_idx


def _classify_trust_tier(score):
    if score >= 75.0:
        return "TRUSTED"
    if score >= 40.0:
        return "STANDARD"
    if score >= 20.0:
        return "PROBATION"
    return "QUARANTINED"


def _is_eligible_for_primary(peer_dict):
    tier = peer_dict.get("trust_tier") or ""
    if tier in ("QUARANTINED", "PROBATION"):
        return False
    score = float(peer_dict.get("trust_score") or 60.0)
    return score >= 40.0


def _ensure_peer_reputation_locked(peer_id, row=-1, col=0):
    rec = _reputation_records.get(peer_id)
    if not rec:
        rec = {
            "peer_id": peer_id,
            "row": row,
            "col": col,
            "trust_score": 60.0,
            "trust_tier": "STANDARD",
            "blocks_completed": 0,
            "blocks_missed": 0,
            "das_served_count": 0,
            "soft_fault_count": 0,
            "byzantine_strikes": 0,
            "straggler_forgiven": 0,
            "last_event": "joined",
            "last_updated_unix": int(time.time()),
        }
        _reputation_records[peer_id] = rec
    else:
        if row >= 0:
            rec["row"] = row
        if col >= 0:
            rec["col"] = col
    return rec


def _apply_reputation_event_locked(peer_id, event_type, block_id="", count=1, row=-1, col=0):
    count = max(1, int(count or 1))
    rec = _ensure_peer_reputation_locked(peer_id, row=row, col=col)
    for _ in range(count):
        if event_type == "block_completed":
            if block_id:
                comp_set = _rep_block_completed.setdefault(block_id, set())
                if peer_id in comp_set and count == 1:
                    break
                comp_set.add(peer_id)
                miss_set = _rep_block_missed.get(block_id)
                if miss_set and peer_id in miss_set:
                    miss_set.discard(peer_id)
                    rec["blocks_missed"] = max(0, rec["blocks_missed"] - 1)
                    rec["soft_fault_count"] = max(0, rec["soft_fault_count"] - 1)
                    rec["straggler_forgiven"] += 1
                    rec["trust_score"] = min(100.0, rec["trust_score"] + 8.0)
            if rec["trust_tier"] != "QUARANTINED":
                rec["blocks_completed"] += 1
                rec["trust_score"] = min(100.0, rec["trust_score"] + 2.0)
        elif event_type == "das_served":
            if rec["trust_tier"] != "QUARANTINED":
                rec["das_served_count"] += 1
                rec["trust_score"] = min(100.0, rec["trust_score"] + 0.5)
        elif event_type == "missed_block":
            if block_id:
                if peer_id in _rep_block_completed.get(block_id, set()):
                    continue
                miss_set = _rep_block_missed.setdefault(block_id, set())
                if peer_id in miss_set and count == 1:
                    continue
                miss_set.add(peer_id)
            if rec["trust_tier"] != "QUARANTINED":
                rec["blocks_missed"] += 1
                rec["soft_fault_count"] += 1
                rec["trust_score"] = max(25.0, rec["trust_score"] - 8.0)
        elif event_type == "fetch_timeout":
            if rec["trust_tier"] != "QUARANTINED":
                rec["soft_fault_count"] += 1
                rec["trust_score"] = max(25.0, rec["trust_score"] - 3.0)
        elif event_type == "ungraceful_drop":
            if rec["trust_tier"] != "QUARANTINED":
                rec["soft_fault_count"] += 1
                rec["trust_score"] = max(25.0, rec["trust_score"] - 10.0)
        elif event_type in ("byzantine_fault", "griefing_fault"):
            rec["byzantine_strikes"] += 1
            rec["trust_score"] = 0.0
        elif event_type == "manual_reset":
            rec["trust_score"] = 60.0
            rec["byzantine_strikes"] = 0
            rec["soft_fault_count"] = 0
            rec["blocks_missed"] = 0

    rec["trust_score"] = round(float(rec["trust_score"]), 1)
    rec["trust_tier"] = _classify_trust_tier(rec["trust_score"])
    rec["last_event"] = event_type
    rec["last_updated_unix"] = int(time.time())
    return dict(rec)


def _compute_trust_weighted_custody_map(peers, net_rows=8):
    active = [
        dict(p) for p in peers
        if int(p.get("row", -1)) >= 0 and p.get("peer_id") and p.get("trust_tier") != "QUARANTINED" and float(p.get("trust_score", 60.0)) >= 20.0
    ]
    active.sort(key=lambda p: (int(p.get("row", 0)), -float(p.get("trust_score", 60.0)), p.get("peer_id", "")))
    custody_map = {}
    if not active:
        return custody_map

    grouped = {r: [] for r in range(net_rows)}
    for p in active:
        r = int(p.get("row", 0)) % net_rows
        grouped[r].append(p)

    borrow_count = {}

    def pick_best_borrowed(target_row, used_ids):
        best_peer = None
        best_tuple = None
        for offset in range(1, net_rows):
            cand_row = (target_row + offset) % net_rows
            cand_peers = grouped.get(cand_row) or []
            if not cand_peers:
                continue
            surplus_tier = 1 if len(cand_peers) >= 2 else 0
            start_idx = (target_row + offset) % len(cand_peers) if len(cand_peers) > 1 else 0
            for i in range(len(cand_peers)):
                cand = cand_peers[(start_idx + i) % len(cand_peers)]
                cid = cand["peer_id"]
                if cid in used_ids:
                    continue
                healthy_tier = 1 if _is_eligible_for_primary(cand) else 0
                b_cnt = borrow_count.get(cid, 0)
                c_score = float(cand.get("trust_score", 60.0))
                cand_tuple = (healthy_tier, surplus_tier, -b_cnt, c_score, -offset)
                if best_tuple is None or cand_tuple > best_tuple:
                    best_tuple = cand_tuple
                    best_peer = cand
        return best_peer

    for target_row in range(net_rows):
        cell_peers = list(grouped.get(target_row) or [])
        if len(cell_peers) >= 2:
            assigned = list(cell_peers)
            has_healthy = any(_is_eligible_for_primary(p) for p in assigned)
            if not has_healthy:
                used = {p["peer_id"] for p in assigned}
                borrowed = pick_best_borrowed(target_row, used)
                if borrowed and _is_eligible_for_primary(borrowed):
                    assigned = [borrowed] + assigned
                    borrow_count[borrowed["peer_id"]] = borrow_count.get(borrowed["peer_id"], 0) + 1
            custody_map[str(target_row)] = assigned
            continue

        if len(cell_peers) == 1:
            assigned = [cell_peers[0]]
            used = {cell_peers[0]["peer_id"]}
            borrowed = pick_best_borrowed(target_row, used)
            if borrowed:
                assigned.append(borrowed)
                borrow_count[borrowed["peer_id"]] = borrow_count.get(borrowed["peer_id"], 0) + 1
            custody_map[str(target_row)] = assigned
            continue

        assigned = []
        used = set()
        while len(assigned) < 2:
            borrowed = pick_best_borrowed(target_row, used)
            if not borrowed:
                break
            used.add(borrowed["peer_id"])
            assigned.append(borrowed)
            borrow_count[borrowed["peer_id"]] = borrow_count.get(borrowed["peer_id"], 0) + 1
        custody_map[str(target_row)] = assigned

    return custody_map


def _compute_cell_striping_preview(assigned_peers, target_net_row, net_rows=8, total_data_rows=128, sample_block_id="block-1"):
    sub_rows = max(1, total_data_rows // max(1, net_rows))
    m = len(assigned_peers)
    if m == 0:
        return {"sub_rows_per_cell": sub_rows, "active_committee_size": 0, "per_peer_load": {}, "schedule_sample": []}
    if m == 1:
        pid = assigned_peers[0].get("peer_id", "")
        return {
            "sub_rows_per_cell": sub_rows,
            "active_committee_size": 1,
            "per_peer_load": {pid: {"primary_rows": sub_rows, "backup_rows": 0, "non_custody_rows": 0}},
            "schedule_sample": [{"sub_row": 0, "data_row": target_net_row, "primary": pid, "backup": None}],
        }

    p0_row = int(assigned_peers[0].get("row", 0)) % net_rows
    p1_row = int(assigned_peers[1].get("row", 0)) % net_rows
    is_single_native_with_borrowed = (m == 2 and p0_row == target_net_row and p1_row != target_net_row)
    co_retain_backup = is_single_native_with_borrowed and (not _is_eligible_for_primary(assigned_peers[0])) and _is_eligible_for_primary(assigned_peers[1])

    healthy_pool = [p for p in assigned_peers if _is_eligible_for_primary(p)]
    probation_pool = [p for p in assigned_peers if not _is_eligible_for_primary(p)]

    per_peer = {
        p.get("peer_id", ""): {
            "primary_rows": 0,
            "backup_rows": 0,
            "non_custody_rows": 0,
            "is_borrowed": (int(p.get("row", 0)) % net_rows) != target_net_row,
            "co_retain_guardian": False,
        }
        for p in assigned_peers
    }
    schedule = []
    for sr in range(sub_rows):
        data_row = sr * net_rows + target_net_row
        if is_single_native_with_borrowed:
            p_id = assigned_peers[0].get("peer_id", "")
            b_id = assigned_peers[1].get("peer_id", "")
            if co_retain_backup:
                per_peer[b_id]["co_retain_guardian"] = True
        else:
            rot = _fnv1a_64_custody_offset(sample_block_id, target_net_row, sr)
            if len(healthy_pool) >= 2:
                p_idx = rot % len(healthy_pool)
                b_idx = (p_idx + 1) % len(healthy_pool)
                p_id = healthy_pool[p_idx].get("peer_id", "")
                b_id = healthy_pool[b_idx].get("peer_id", "")
            elif len(healthy_pool) == 1 and len(probation_pool) >= 1:
                p_id = healthy_pool[0].get("peer_id", "")
                b_id = probation_pool[rot % len(probation_pool)].get("peer_id", "")
            else:
                p_idx = rot % m
                b_idx = (p_idx + 1) % m
                p_id = assigned_peers[p_idx].get("peer_id", "")
                b_id = assigned_peers[b_idx].get("peer_id", "")

        for peer in assigned_peers:
            pid = peer.get("peer_id", "")
            if pid == p_id:
                per_peer[pid]["primary_rows"] += 1
            elif pid == b_id:
                per_peer[pid]["backup_rows"] += 1
            else:
                per_peer[pid]["non_custody_rows"] += 1
        if len(schedule) < 6:
            schedule.append({
                "sub_row": sr,
                "data_row": data_row,
                "primary": p_id,
                "backup": b_id,
                "co_retain_guardian": co_retain_backup,
            })

    return {
        "sub_rows_per_cell": sub_rows,
        "active_committee_size": 2,
        "co_retain_guardian": co_retain_backup,
        "per_peer_load": per_peer,
        "schedule_sample": schedule,
    }


def _get_network_matrix_status(sample_block_id="block-1"):
    boot_data = {}
    try:
        boot_data = _http_get_json(f"{BOOTSTRAP_URL}/bootstrap/peers", timeout=1.5)
    except Exception as e:
        boot_data = {"error": str(e)}

    net_rows = int(boot_data.get("net_rows") or 8)
    col_id = int(boot_data.get("col_id") or 0)
    raw_custody_map = boot_data.get("row_custody_map") or {}

    # Extract unique PeerInfo objects from row_custody_map (and any dict items in peers)
    peers_by_id = {}
    for _, c_list in raw_custody_map.items():
        for cp in (c_list or []):
            if isinstance(cp, dict) and cp.get("peer_id"):
                peers_by_id[cp["peer_id"]] = cp
    for p in (boot_data.get("peers") or []):
        if isinstance(p, dict) and p.get("peer_id"):
            peers_by_id[p["peer_id"]] = p
    peers = list(peers_by_id.values())

    # Inspect spawned store nodes and clean up any exited processes
    with _nodes_lock:
        for port, info in list(_spawned_store_nodes.items()):
            if info["proc"].poll() is not None:
                _spawned_store_nodes.pop(port, None)
        for port, info in list(_spawned_light_nodes.items()):
            if info["proc"].poll() is not None:
                _spawned_light_nodes.pop(port, None)
        spawned_store_ports = set(_spawned_store_nodes.keys())
        spawned_light_ports = set(_spawned_light_nodes.keys())

    # Discover store node HTTP ports from multiaddrs (e.g. store-0-1 -> 9300, or p2p_port - 10000)
    peer_port_map = {}
    for p in peers:
        pid = p.get("peer_id") or ""
        maddrs = p.get("multiaddrs") or []
        http_port = None
        for ma in maddrs:
            m_docker = re.search(r"store-(\d+)-(\d+)", ma)
            if m_docker:
                idx = int(m_docker.group(2))
                http_port = 9300 + idx - 1
                break
            m = re.search(r"/(?:tcp|udp)/(\d+)", ma)
            if m:
                p2p_p = int(m.group(1))
                if p2p_p > 10000 and p2p_p != 18080:
                    http_port = p2p_p - 10000
                    break
        if http_port:
            peer_port_map[pid] = http_port

    # Enrich each peer with reputation records and compute trust-weighted custody map
    with _rep_lock:
        for p in peers:
            pid = p.get("peer_id") or ""
            rec = _ensure_peer_reputation_locked(pid, row=int(p.get("row", 0)), col=int(p.get("col", 0)))
            if isinstance(p.get("trust_score"), (int, float)) and p.get("trust_score") > 0 and rec["last_event"] == "joined":
                rec["trust_score"] = round(float(p["trust_score"]), 1)
                rec["trust_tier"] = p.get("trust_tier") or _classify_trust_tier(rec["trust_score"])
            p["trust_score"] = rec["trust_score"]
            p["trust_tier"] = rec["trust_tier"]
            p["blocks_completed"] = rec["blocks_completed"]
            p["blocks_missed"] = rec["blocks_missed"]
            p["straggler_forgiven"] = rec["straggler_forgiven"]
            p["soft_fault_count"] = rec["soft_fault_count"]
            p["byzantine_strikes"] = rec["byzantine_strikes"]
            p["last_event"] = rec["last_event"]

    raw_custody_map = _compute_trust_weighted_custody_map(peers, net_rows=net_rows)

    store_nodes = []
    for p in peers:
        pid = p.get("peer_id") or ""
        row = int(p.get("row", 0))
        col = int(p.get("col", 0))
        net_row = row % net_rows
        http_port = peer_port_map.get(pid)
        node_info = None
        if http_port:
            try:
                node_info = _http_get_json(f"http://127.0.0.1:{http_port}/store/info", timeout=0.6)
            except Exception:
                pass
        custody_rows = []
        for r_str, c_list in raw_custody_map.items():
            if any((cp.get("peer_id") == pid) for cp in (c_list or [])):
                try:
                    custody_rows.append(int(r_str))
                except Exception:
                    pass
        custody_rows.sort()
        store_nodes.append({
            "peer_id": pid,
            "port": http_port,
            "p2p_port": (http_port + 10000) if http_port else None,
            "row": row,
            "net_row": net_row,
            "col": col,
            "trust_score": p.get("trust_score", 60.0),
            "trust_tier": p.get("trust_tier", "STANDARD"),
            "blocks_completed": p.get("blocks_completed", 0),
            "blocks_missed": p.get("blocks_missed", 0),
            "straggler_forgiven": p.get("straggler_forgiven", 0),
            "soft_fault_count": p.get("soft_fault_count", 0),
            "byzantine_strikes": p.get("byzantine_strikes", 0),
            "last_event": p.get("last_event", "joined"),
            "custody_net_rows": custody_rows or (node_info.get("custody_net_rows") if node_info else []),
            "total_stored_pieces": node_info.get("total_stored_pieces") if node_info else None,
            "custody_pieces": node_info.get("custody_pieces") if node_info else None,
            "recoded_pieces": node_info.get("recoded_pieces") if node_info else None,
            "completed_blocks_count": node_info.get("completed_blocks_count") if node_info else None,
            "is_spawned": bool(http_port and http_port in spawned_store_ports),
            "healthy": p.get("trust_tier") != "QUARANTINED",
        })

    store_nodes.sort(key=lambda x: (x["net_row"], -float(x.get("trust_score") or 60.0), x["port"] or 0, x["peer_id"]))

    # Build 2D cell breakdown for each row 0..net_rows-1
    cells = []
    for r in range(net_rows):
        native_peers = [s for s in store_nodes if s["net_row"] == r and s["trust_tier"] != "QUARANTINED"]
        assigned_custody = raw_custody_map.get(str(r)) or []
        striping = _compute_cell_striping_preview(assigned_custody, r, net_rows=net_rows, total_data_rows=128, sample_block_id=sample_block_id)
        cells.append({
            "net_row": r,
            "net_col": col_id,
            "native_count": len(native_peers),
            "native_peers": native_peers,
            "custody_pool_count": len(assigned_custody),
            "custody_pool": assigned_custody,
            "striping": striping,
        })

    # Discover active Light Nodes (port 9401 + spawned light nodes)
    candidate_light_ports = sorted(set([9401] + list(spawned_light_ports)))
    light_nodes = []
    for lport in candidate_light_ports:
        l_status = None
        healthy = False
        try:
            l_status = _http_get_json(f"http://127.0.0.1:{lport}/light/status", timeout=0.8)
            healthy = True
        except Exception:
            try:
                txt = _http_get_text(f"http://127.0.0.1:{lport}/health", timeout=0.6)
                if "healthy" in txt.lower():
                    healthy = True
            except Exception:
                pass
        if healthy:
            light_nodes.append({
                "port": lport,
                "p2p_port": lport + 10000,
                "peer_id": (l_status or {}).get("peer_id") or f"cda-light-{lport}",
                "cached_headers_count": (l_status or {}).get("cached_headers_count"),
                "das_history": (l_status or {}).get("das_history") or [],
                "is_spawned": lport in spawned_light_ports,
                "healthy": True,
            })

    return {
        "status": "success",
        "col_id": col_id,
        "net_rows": net_rows,
        "net_cols": 8,
        "k_block": 64,
        "k_piece": 4,
        "data_rows_per_block": 128,
        "sub_rows_per_cell": 128 // max(1, net_rows),
        "sample_block_id": sample_block_id,
        "active_peers_in_col": len(store_nodes),
        "cells": cells,
        "store_nodes": store_nodes,
        "light_nodes": light_nodes,
    }


def _spawn_store_node(req_data):
    row = int(req_data.get("row", -1))
    col = int(req_data.get("col", 0))
    store_bin = os.path.join(REPO_ROOT, "bin", "store")
    if not os.path.exists(store_bin):
        return {"status": "error", "message": f"Store binary not found at {store_bin}"}

    with _nodes_lock:
        chosen_port = None
        for p in range(9310, 9360):
            if p not in _spawned_store_nodes and not _is_port_in_use(p) and not _is_port_in_use(p + 10000):
                chosen_port = p
                break
        if chosen_port is None:
            return {"status": "error", "message": "No available ports in range 9310..9359"}

        # Clean up any old key/data for this port so fresh coordinates take effect cleanly
        key_file = os.path.join(REPO_ROOT, f"store_{chosen_port}.key")
        if os.path.exists(key_file):
            try:
                os.remove(key_file)
            except Exception:
                pass

        log_path = os.path.join(REPO_ROOT, f"data/store_{chosen_port}_spawn.log")
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        log_f = open(log_path, "ab")
        cmd = [
            store_bin,
            "-port", str(chosen_port),
            "-row", str(row),
            "-col", str(col),
            "-publisher", PUB_URL,
            "-bootstrap", BOOTSTRAP_URL,
            "-k", "64",
            "-k-piece", "4",
            "-net-rows", "8",
            "-net-cols", "8",
            "-myaddr", f"http://127.0.0.1:{chosen_port}",
        ]
        proc = subprocess.Popen(cmd, cwd=REPO_ROOT, stdout=log_f, stderr=subprocess.STDOUT)
        _spawned_store_nodes[chosen_port] = {
            "proc": proc,
            "row": row,
            "col": col,
            "started_at": time.time(),
            "log_path": log_path,
        }

    # Wait up to 2.5s for the Store Node to register with Bootstrap and start its HTTP server
    node_info = None
    for _ in range(10):
        time.sleep(0.25)
        try:
            node_info = _http_get_json(f"http://127.0.0.1:{chosen_port}/store/info", timeout=0.5)
            if node_info and node_info.get("peer_id"):
                break
        except Exception:
            pass

    return {
        "status": "success",
        "port": chosen_port,
        "p2p_port": chosen_port + 10000,
        "requested_row": row,
        "node_info": node_info,
    }


def _stop_store_node(req_data):
    port = int(req_data.get("port", 0))
    with _nodes_lock:
        entry = _spawned_store_nodes.pop(port, None)
    if not entry:
        return {"status": "error", "message": f"Store Node on port {port} is not a dynamically spawned process (only nodes spawned from dashboard on ports 9310+ can be stopped here)."}

    proc = entry["proc"]
    try:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=2.5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass

    key_file = os.path.join(REPO_ROOT, f"store_{port}.key")
    data_dir = os.path.join(REPO_ROOT, "data", f"store_{port}")
    for path in (key_file, data_dir):
        try:
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=True)
            elif os.path.exists(path):
                os.remove(path)
        except Exception:
            pass

    time.sleep(0.4)
    return {"status": "success", "port": port, "message": f"Store Node on port {port} gracefully deregistered and stopped."}


def _spawn_light_node(req_data):
    light_bin = os.path.join(REPO_ROOT, "bin", "light")
    if not os.path.exists(light_bin):
        return {"status": "error", "message": f"Light binary not found at {light_bin}"}

    with _nodes_lock:
        chosen_port = None
        for p in range(9402, 9430):
            if p not in _spawned_light_nodes and not _is_port_in_use(p) and not _is_port_in_use(p + 10000):
                chosen_port = p
                break
        if chosen_port is None:
            return {"status": "error", "message": "No available Light Node ports in range 9402..9429"}

        log_path = os.path.join(REPO_ROOT, f"data/light_{chosen_port}_spawn.log")
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        log_f = open(log_path, "ab")
        cmd = [
            light_bin,
            "-port", str(chosen_port),
            "-publisher", PUB_URL,
            "-bootstraps", f"0:{BOOTSTRAP_URL}",
            "-k", "64",
            "-k-piece", "4",
            "-net-cols", "8",
            "-net-rows", "8",
            "-auto-das=true",
        ]
        proc = subprocess.Popen(cmd, cwd=REPO_ROOT, stdout=log_f, stderr=subprocess.STDOUT)
        _spawned_light_nodes[chosen_port] = {
            "proc": proc,
            "started_at": time.time(),
            "log_path": log_path,
        }

    l_status = None
    for _ in range(8):
        time.sleep(0.25)
        try:
            l_status = _http_get_json(f"http://127.0.0.1:{chosen_port}/light/status", timeout=0.5)
            if l_status:
                break
        except Exception:
            pass

    return {
        "status": "success",
        "port": chosen_port,
        "p2p_port": chosen_port + 10000,
        "light_status": l_status,
    }


def _stop_light_node(req_data):
    port = int(req_data.get("port", 0))
    with _nodes_lock:
        entry = _spawned_light_nodes.pop(port, None)
    if not entry:
        return {"status": "error", "message": f"Light Node on port {port} is not a dynamically spawned process."}
    proc = entry["proc"]
    try:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=2.0)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass
    return {"status": "success", "port": port, "message": f"Light Node on port {port} stopped."}


def _trigger_light_das_sample(req_data):
    port = int(req_data.get("port") or 9401)
    block_id = str(req_data.get("block_id") or "").strip()
    if not block_id:
        return {"status": "error", "message": "Missing block_id"}
    if not block_id.startswith("block-"):
        block_id = f"block-{block_id}"

    row = req_data.get("row")
    col = req_data.get("col")
    samples = int(req_data.get("samples") or 4)

    if row is not None and row != "" and col is not None and col != "":
        url = f"http://127.0.0.1:{port}/das/sample/{block_id}?row={int(row)}&col={int(col)}"
    else:
        url = f"http://127.0.0.1:{port}/das/sample/{block_id}?samples={max(1, min(32, samples))}"

    t0 = time.time()
    try:
        das_res = _http_get_json(url, timeout=15.0)
        elapsed_ms = round((time.time() - t0) * 1000, 1)
        return {
            "status": "success",
            "port": port,
            "block_id": block_id,
            "duration_ms": elapsed_ms,
            "das": das_res,
        }
    except Exception as e:
        return {"status": "error", "message": f"DAS Sampling on Light Node :{port} failed: {e}"}


def _handle_reputation_event(req_data):
    target_query = str(req_data.get("peer_id") or req_data.get("target") or "").strip()
    target_row = req_data.get("row")
    target_port = req_data.get("port")
    event_type = str(req_data.get("event") or "block_completed").strip()
    block_id = str(req_data.get("block_id") or "").strip()
    count = int(req_data.get("count") or 1)

    valid_events = {
        "block_completed", "das_served", "missed_block",
        "fetch_timeout", "ungraceful_drop", "byzantine_fault",
        "griefing_fault", "manual_reset",
    }
    if event_type not in valid_events:
        return {"status": "error", "message": f"Invalid event '{event_type}'. Valid: {sorted(valid_events)}"}

    # Ensure current peers are loaded
    matrix = _get_network_matrix_status(sample_block_id=block_id or "block-5")
    store_nodes = matrix.get("store_nodes") or []

    if target_query.lower() == "all":
        updated = []
        with _rep_lock:
            for s in store_nodes:
                rec = _apply_reputation_event_locked(s["peer_id"], event_type, block_id=block_id, count=count, row=s["row"], col=s["col"])
                updated.append(rec)
        new_matrix = _get_network_matrix_status(sample_block_id=block_id or "block-5")
        return {
            "status": "success",
            "updated_records": updated,
            "cells": new_matrix.get("cells"),
            "store_nodes": new_matrix.get("store_nodes"),
        }

    matched_node = None
    for s in store_nodes:
        if target_port is not None and str(s.get("port")) == str(target_port):
            matched_node = s
            break
        if target_query and (str(s.get("port")) == target_query or s["peer_id"] == target_query or target_query in s["peer_id"]):
            matched_node = s
            break
        if not target_query and target_row is not None and int(s.get("net_row", -1)) == int(target_row):
            matched_node = s
            break

    if not matched_node:
        return {"status": "error", "message": f"No active Store Node matched target='{target_query}' port='{target_port}' row='{target_row}'"}

    pid = matched_node["peer_id"]
    with _rep_lock:
        updated_rec = _apply_reputation_event_locked(pid, event_type, block_id=block_id, count=count, row=matched_node["row"], col=matched_node["col"])

    # Forward to Bootstrap /bootstrap/reputation/event if supported by running Bootstrap binary
    try:
        b_req = urllib.request.Request(
            f"{BOOTSTRAP_URL}/bootstrap/reputation/event",
            data=json.dumps({"peer_id": pid, "row": matched_node["row"], "event": event_type, "block_id": block_id, "count": count}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(b_req, timeout=0.8).read()
    except Exception:
        pass

    new_matrix = _get_network_matrix_status(sample_block_id=block_id or "block-5")
    return {
        "status": "success",
        "updated_record": updated_rec,
        "cells": new_matrix.get("cells"),
        "store_nodes": new_matrix.get("store_nodes"),
    }


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
                            # Credit active non-quarantined Store Nodes (+2.0) when a live block completes DA quorum
                            try:
                                m_now = _get_network_matrix_status(sample_block_id=f"block-{h}")
                                with _rep_lock:
                                    for sn in (m_now.get("store_nodes") or []):
                                        if sn.get("trust_tier") != "QUARANTINED":
                                            _apply_reputation_event_locked(sn["peer_id"], "block_completed", block_id=f"block-{h}", count=1, row=sn["row"], col=sn["col"])
                            except Exception:
                                pass
        except Exception:
            time.sleep(2.0)


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB_DIR, **kwargs)

    def _normalize_path(self):
        clean_path = self.path.split('?')[0].rstrip('/')
        if clean_path in ("", "/", "/dashboard", "/dashboadrd", "/dash"):
            self.path = "/index.html"
        elif clean_path in ("/network-admin", "/admin", "/node-admin"):
            self.path = "/network_admin.html"
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

        if clean_path == "/api/network-matrix":
            try:
                sample_block = "block-1"
                if "?" in self.path:
                    query = self.path.split("?", 1)[1]
                    for param in query.split("&"):
                        if param.startswith("block_id="):
                            sample_block = param.split("=", 1)[1].strip() or "block-1"
                result = _get_network_matrix_status(sample_block)
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

    def do_POST(self):
        clean_path = self.path.split('?')[0].rstrip('/')
        content_len = int(self.headers.get("Content-Length") or 0)
        raw_body = self.rfile.read(content_len) if content_len > 0 else b"{}"
        try:
            req_data = json.loads(raw_body.decode("utf-8")) if raw_body else {}
        except Exception:
            req_data = {}

        handlers = {
            "/api/nodes/store/spawn": _spawn_store_node,
            "/api/nodes/store/stop": _stop_store_node,
            "/api/nodes/light/spawn": _spawn_light_node,
            "/api/nodes/light/stop": _stop_light_node,
            "/api/nodes/light/sample": _trigger_light_das_sample,
            "/api/reputation/event": _handle_reputation_event,
        }
        handler = handlers.get(clean_path)
        if handler:
            try:
                result = handler(req_data)
                code = 200 if result.get("status") == "success" else 400
                body = json.dumps(result).encode("utf-8")
                self.send_response(code)
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

        self.send_response(404)
        self.end_headers()

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
