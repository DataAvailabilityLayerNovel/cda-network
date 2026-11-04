#!/usr/bin/env python3
"""
file_pipeline_bot.py - CDA Network File Ingestion, Chunking & Verification Bot.

Handles:
1. Namespace & Blob Ingestion: Splits files into ~500 KB chunks and ~500B slices
   under URI key format: cda/<namespace>/<blob_id>/s/<chunk>_<slice>.
2. On-Chain Head Manifest: Commits metadata manifest (cda/<namespace>/<blob_id>/meta)
   in the first block, making data discovery 100% self-describing and stateless.
3. Pipelined Submission: Submits chunks consecutively into CometBFT blocks while
   tracking CDA BlockReady and KZG commitments asynchronously.
4. Stateless Retrieval: Reconstructs files from URI (cda://<namespace>/<blob_id>)
   without requiring offline local JSON manifests.
5. Catalog Discovery: Lists all registered blobs in a namespace via Publisher API
   or CometBFT search.
"""

import argparse
import base64
import concurrent.futures
import hashlib
import json
import math
import os
import queue
import re
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


def parse_size_str(size_str):
    """Converts strings like '2MB', '1.5M', '500KB', '1048576' to integer bytes."""
    if isinstance(size_str, int):
        return size_str
    s = str(size_str).strip().upper()
    if s.endswith("MB") or s.endswith("M"):
        val = float(re.sub(r"[A-Z]", "", s))
        return int(val * 1024 * 1024)
    if s.endswith("KB") or s.endswith("K"):
        val = float(re.sub(r"[A-Z]", "", s))
        return int(val * 1024)
    if s.endswith("GB") or s.endswith("G"):
        val = float(re.sub(r"[A-Z]", "", s))
        return int(val * 1024 * 1024 * 1024)
    if s.endswith("B"):
        s = s[:-1]
    return int(s)


def parse_cda_uri(uri_str):
    """Parses URIs like 'cda://namespace/blob_id', 'cda/namespace/blob_id', or 'namespace/blob_id'."""
    clean = uri_str.strip()
    if clean.startswith("cda://"):
        clean = clean[len("cda://"):]
    elif clean.startswith("cda/"):
        clean = clean[len("cda/"):]
    parts = clean.split("/")
    if len(parts) >= 2:
        return parts[0], parts[1]
    raise ValueError(f"Invalid CDA URI '{uri_str}'. Expected format: 'cda://<namespace>/<blob_id>'")


def submit_tx_to_cometbft(cometbft_rpc, tx_bytes, timeout=30):
    if isinstance(tx_bytes, str):
        tx_bytes = tx_bytes.encode('utf-8')
    b64_tx = base64.b64encode(tx_bytes).decode('ascii')
    payload = {
        "jsonrpc": "2.0",
        "id": int(time.time() * 1000) % 1000000,
        "method": "broadcast_tx_sync",
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


def broadcast_tx_batch(cometbft_rpc, txs, max_workers=32, timeout=30):
    hashes = [None] * len(txs)

    def send_one(idx, tx):
        res = submit_tx_to_cometbft(cometbft_rpc, tx, timeout=timeout)
        tx_res = res.get("result", {})
        code = tx_res.get("code", 0)
        if code != 0:
            log_msg = str(tx_res.get('log', ''))
            if "mempool is full" in log_msg.lower():
                raise RuntimeError("MempoolIsFull")
            raise RuntimeError(f"CheckTx rejected tx with code {code}: {log_msg}")
        return idx, tx_res.get("hash", "")

    if len(txs) == 1:
        _, h = send_one(0, txs[0])
        return h, h, [h]

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(send_one, i, tx): i
            for i, tx in enumerate(txs)
        }
        for future in concurrent.futures.as_completed(future_to_idx):
            i, h = future.result()
            hashes[i] = h

    first_tx_hash = hashes[0] if hashes else None
    last_tx_hash = hashes[-1] if hashes else None
    return first_tx_hash, last_tx_hash, hashes


def wait_for_tx_commit(cometbft_rpc, tx_hash, timeout=60, poll_interval=0.1):
    url = f"{cometbft_rpc}/tx?hash=0x{tx_hash}&prove=false"
    start = time.time()
    while time.time() - start < timeout:
        try:
            req = urllib.request.Request(url, headers={'Accept': 'application/json'})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode('utf-8'))
                res = data.get("result", {})
                if res.get("height"):
                    return int(res["height"])
        except urllib.error.HTTPError:
            pass
        except Exception:
            pass
        time.sleep(poll_interval)
    raise TimeoutError(f"Tx 0x{tx_hash} was not committed within {timeout}s")


def fetch_publisher_block_info(publisher_url, block_id, max_retries=5):
    url = f"{publisher_url}/header/{block_id}"
    for attempt in range(max_retries):
        try:
            data = http_get_json(url, timeout=3)
            if data and "commits_root" in data:
                k_val = len(data.get("column_comm", [])) // 2
                return {
                    "commits_root": data.get("commits_root", ""),
                    "k": k_val if k_val > 0 else 32
                }
        except Exception:
            time.sleep(0.5)
    return None


def wait_for_block_ready(bootstrap_url, expected_block_id, publisher_url=None, timeout=120, poll_interval=0.1):
    start = time.time()
    url = f"{bootstrap_url}/ready"
    last_h = 0
    exp_h = 0
    if expected_block_id and expected_block_id.startswith("block-"):
        try:
            exp_h = int(expected_block_id.split("-")[1])
        except Exception:
            exp_h = 0

    while time.time() - start < timeout:
        try:
            req = urllib.request.Request(url, headers={'Accept': 'application/json'})
            with urllib.request.urlopen(req, timeout=2) as resp:
                data = json.loads(resp.read().decode('utf-8'))
                b_id = data.get("block_id")
                h = data.get("height", 0)
                if h > last_h:
                    last_h = h

                is_target_block = False
                if expected_block_id and b_id == expected_block_id:
                    is_target_block = True

                if is_target_block:
                    if publisher_url:
                        try:
                            b_url = f"{publisher_url}/block/{expected_block_id}"
                            b_info = http_get_json(b_url, timeout=2)
                            if b_info and b_info.get("status") == "success":
                                if b_info.get("da_quorum_ready", False):
                                    return True, time.time() - start, data.get("k", b_info.get("k", 0))
                        except Exception:
                            pass
                    else:
                        return True, time.time() - start, data.get("k", 0)
        except Exception:
            pass

        if publisher_url:
            try:
                b_url = f"{publisher_url}/block/{expected_block_id}"
                binfo = http_get_json(b_url, timeout=2)
                if binfo and binfo.get("status") == "success":
                    if binfo.get("da_quorum_ready", False):
                        return True, time.time() - start, binfo.get("k", 0)
            except Exception:
                pass

        time.sleep(poll_interval)
    return False, time.time() - start, 0


# ------------------------------------------------------------------------------
# RETRIEVAL MODULE
# ------------------------------------------------------------------------------

def fetch_single_tx_by_key(publisher_url, key, timeout=10):
    url = f"{publisher_url}/tx?key={urllib.parse.quote(key)}"
    data = http_get_json(url, timeout=timeout)
    if data.get("status") == "success" and data.get("payload"):
        payload = data["payload"]
        if "=" in payload:
            return payload.split("=", 1)[1]
        return payload
    raise KeyError(f"Key {key} not found: {data.get('message')}")


def list_blobs_in_namespace(namespace, publisher_url, cometbft_rpc):
    print("=" * 80)
    print(f"CDA NETWORK - NAMESPACE CATALOG: '{namespace}'")
    print(f"Publisher API: {publisher_url}")
    print("=" * 80)

    blobs = []
    # 1. Try Publisher API
    try:
        url = f"{publisher_url}/api/v1/namespaces/{namespace}/blobs"
        resp = http_get_json(url, timeout=5)
        if resp.get("status") == "success" and "blobs" in resp:
            blobs = resp["blobs"]
    except Exception:
        pass

    # 2. Fallback to CometBFT tx_search
    if not blobs:
        try:
            query = urllib.parse.quote(f"app.key CONTAINS 'cda/{namespace}/' AND app.key CONTAINS '/meta'")
            url = f"{cometbft_rpc}/tx_search?query=\"{query}\"&per_page=50"
            resp = http_get_json(url, timeout=10)
            txs = resp.get("result", {}).get("txs", [])
            for t in txs:
                raw_b64 = t.get("tx", "")
                decoded = base64.b64decode(raw_b64).decode('utf-8', errors='ignore')
                if "=" in decoded:
                    k, v = decoded.split("=", 1)
                    parts = k.split("/")
                    b_id = parts[2] if len(parts) >= 3 else "unknown"
                    try:
                        try:
                            raw_bytes = bytes.fromhex(v)
                            m_obj = json.loads(raw_bytes.decode('utf-8'))
                        except Exception:
                            m_obj = json.loads(v)
                        blobs.append({
                            "blob_id": b_id,
                            "filename": m_obj.get("filename", ""),
                            "total_size": m_obj.get("total_size", 0),
                            "sha256": m_obj.get("sha256", ""),
                            "total_slices": m_obj.get("total_slices", 0),
                            "status": "READY",
                            "block_heights": [int(t.get("height", 0))]
                        })
                    except Exception:
                        pass
        except Exception as e:
            print(f"Warning querying CometBFT: {e}")

    if not blobs:
        print(f"ℹ️ No blobs found in namespace '{namespace}'.")
        return

    print(f"{'BLOB ID':<20} | {'FILENAME':<24} | {'SIZE (KB)':<10} | {'STATUS':<10} | {'SHA256 (PREFIX)':<16}")
    print("-" * 90)
    for b in blobs:
        b_id = b.get("blob_id", "")
        fn = b.get("filename", "")[:24]
        sz = f"{b.get('total_size', 0) / 1024:.1f}"
        st = b.get("status", "READY")
        sh = b.get("sha256", "")[:16]
        print(f"{b_id:<20} | {fn:<24} | {sz:<10} | {st:<10} | {sh:<16}")
    print("=" * 90)


def retrieve_file_stateless(namespace, blob_id, output_path, publisher_url, cometbft_rpc, workers=32, retrieval_mode="auto"):
    print("=" * 80)
    print("CDA NETWORK - STATELESS BLOB RETRIEVAL & INTEGRITY VERIFICATION")
    print(f"Target URI     : cda://{namespace}/{blob_id}")
    print(f"Publisher API  : {publisher_url}")
    print(f"Retrieval Mode : {retrieval_mode.upper()}")
    print("=" * 80)

    # 1. Fetch on-chain manifest
    meta_key = f"cda/{namespace}/{blob_id}/meta"
    print(f"🔍 Fetching On-Chain Manifest for 'cda://{namespace}/{blob_id}'...")

    manifest = None
    # Try publisher /api/v1/namespaces/{ns}/blobs/{id}
    try:
        url = f"{publisher_url}/api/v1/namespaces/{namespace}/blobs/{blob_id}"
        resp = http_get_json(url, timeout=5)
        if resp.get("status") == "success" and resp.get("blob"):
            b = resp["blob"]
            manifest = {
                "file_id": blob_id,
                "filename": b.get("filename", f"{blob_id}.bin"),
                "file_size": b.get("total_size", 0),
                "file_sha256": b.get("sha256", ""),
                "total_chunks": b.get("total_chunks", 1),
                "chunk_size": b.get("chunk_size", 512000),
                "slice_size": b.get("slice_size", 500),
                "total_slices": b.get("total_slices", 0),
                "block_heights": b.get("block_heights", [])
            }
    except Exception:
        pass

    # Fallback to /tx?key=cda/<ns>/<blob_id>/meta
    if not manifest or manifest.get("file_size", 0) == 0:
        try:
            raw_meta_str = fetch_single_tx_by_key(publisher_url, meta_key, timeout=10)
            try:
                raw_bytes = bytes.fromhex(raw_meta_str)
                m_obj = json.loads(raw_bytes.decode('utf-8'))
            except Exception:
                m_obj = json.loads(raw_meta_str)
            manifest = {
                "file_id": blob_id,
                "filename": m_obj.get("filename", f"{blob_id}.bin"),
                "file_size": m_obj.get("total_size", 0),
                "file_sha256": m_obj.get("sha256", ""),
                "total_chunks": m_obj.get("total_chunks", 1),
                "chunk_size": m_obj.get("chunk_size", 512000),
                "slice_size": m_obj.get("slice_size", 500),
                "total_slices": m_obj.get("total_slices", 0),
                "block_heights": []
            }
        except Exception as e:
            raise RuntimeError(f"Failed to fetch on-chain manifest for cda://{namespace}/{blob_id}: {e}")

    filename = manifest.get("filename", f"{blob_id}.bin")
    expected_size = manifest["file_size"]
    expected_sha256 = manifest["file_sha256"]
    total_chunks = max(1, manifest.get("total_chunks", 1))
    slice_size = manifest.get("slice_size", 500)
    chunk_size = manifest.get("chunk_size", 512000)
    total_slices = manifest.get("total_slices", 0)
    if total_slices == 0 and expected_size > 0:
        total_slices = math.ceil(expected_size / slice_size)

    if not output_path:
        output_path = f"retrieved_{filename}"

    print(f"\n📂 Namespace   : {namespace}")
    print(f"📦 Blob ID     : {blob_id}")
    print(f"📄 Filename    : {filename}")
    print(f"📏 Size        : {expected_size} bytes ({expected_size / 1024 / 1024:.2f} MB)")
    print(f"🧩 Chunks      : {total_chunks}")
    print(f"🍰 Slices      : {total_slices}")
    print(f"🔒 SHA-256     : {expected_sha256}")
    print("-" * 80)

    start_time = time.time()

    # Direction 1.2: Direct Streaming Download
    if retrieval_mode in ["auto", "direct"]:
        try:
            print("⚡ Attempting Direction 1.2: Direct Streaming Download...")
            dl_url = f"{publisher_url}/api/v1/namespaces/{namespace}/blobs/{blob_id}/download"
            req = urllib.request.Request(dl_url)
            with urllib.request.urlopen(req, timeout=30) as resp:
                if resp.status == 200:
                    full_file = resp.read()
                    if len(full_file) == expected_size and (not expected_sha256 or hashlib.sha256(full_file).hexdigest() == expected_sha256):
                        elapsed = time.time() - start_time
                        rate_mb = (len(full_file) / 1024 / 1024) / max(0.001, elapsed)
                        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
                        with open(output_path, "wb") as f:
                            f.write(full_file)
                        print(f"🎉 Direct Download Succeeded in {elapsed:.3f}s ({rate_mb:.2f} MB/s)!")
                        print(f"💾 File saved successfully to: {output_path}")
                        print("=" * 80)
                        return
        except Exception as e:
            if retrieval_mode == "direct":
                raise RuntimeError(f"Direct download failed in 'direct' mode: {e}")
            print(f"⚠️ Direct download unavailable ({e}), trying next retrieval mode...")

    # Collect deterministic slice keys
    all_slice_keys = []
    for c_idx in range(total_chunks):
        offset = c_idx * chunk_size
        actual_chunk_size = min(chunk_size, expected_size - offset)
        num_slices = math.ceil(actual_chunk_size / slice_size)
        for s_idx in range(num_slices):
            key = f"cda/{namespace}/{blob_id}/s/{c_idx:03d}_{s_idx:04d}"
            all_slice_keys.append(key)

    # Direction 1.1: Batch Slice Query (POST /tx/batch)
    if retrieval_mode in ["auto", "batch"]:
        print(f"ℹ️ Retrieving via Direction 1.1: Batch Query API (POST /tx/batch) in 500-key batches ({len(all_slice_keys)} slices)...")
        batch_size = 500
        slice_data_map = {}
        batch_failed = False
        t_batch_start = time.time()

        for i in range(0, len(all_slice_keys), batch_size):
            chunk_keys = all_slice_keys[i:i + batch_size]
            try:
                req_data = json.dumps({"keys": chunk_keys}).encode('utf-8')
                batch_req = urllib.request.Request(
                    f"{publisher_url}/tx/batch",
                    data=req_data,
                    headers={"Content-Type": "application/json"}
                )
                with urllib.request.urlopen(batch_req, timeout=15) as b_resp:
                    if b_resp.status == 200:
                        b_data = json.loads(b_resp.read().decode('utf-8'))
                        if b_data.get("status") == "success":
                            res_map = b_data.get("results", {})
                            for k, v in res_map.items():
                                if "=" in v:
                                    v = v.split("=", 1)[1]
                                slice_data_map[k] = bytes.fromhex(v)
                        else:
                            batch_failed = True
                            break
                    else:
                        batch_failed = True
                        break
            except Exception as e:
                print(f"⚠️ Batch query error: {e}")
                batch_failed = True
                break

        if not batch_failed and len(slice_data_map) == len(all_slice_keys):
            assembled = bytearray()
            for k in all_slice_keys:
                assembled.extend(slice_data_map[k])
            full_file = bytes(assembled[:expected_size])
            calc_sha256 = hashlib.sha256(full_file).hexdigest()
            elapsed = time.time() - start_time
            print(f"\nReconstructed SHA-256 : {calc_sha256}")
            print(f"Expected SHA-256      : {expected_sha256}")

            if expected_sha256 and calc_sha256 != expected_sha256:
                print("❌ INTEGRITY CHECK FAILED: SHA-256 mismatch!")
                sys.exit(1)

            print("🎉 INTEGRITY CHECK PASSED: 100% SHA-256 match!")
            os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
            with open(output_path, "wb") as f:
                f.write(full_file)
            rate_mb = (len(full_file) / 1024 / 1024) / max(0.001, elapsed)
            print(f"💾 File saved successfully to: {output_path}")
            print(f"⏱️ Total Retrieval Time (Batch Query API): {elapsed:.3f}s ({rate_mb:.2f} MB/s)")
            print("=" * 80)
            return

        if retrieval_mode == "batch":
            raise RuntimeError(f"Batch query failed to retrieve all slices ({len(slice_data_map)} / {len(all_slice_keys)} retrieved)")

    # Parallel single slice retrieval (Fallback / Baseline)
    print(f"ℹ️ Reconstructing via parallel single-slice retrieval ({len(all_slice_keys)} slices, {workers} workers)...")
    slices = [None] * len(all_slice_keys)

    def fetch_slice(s_idx, s_key):
        for retry in range(3):
            try:
                hex_val = fetch_single_tx_by_key(publisher_url, s_key, timeout=10)
                return s_idx, bytes.fromhex(hex_val)
            except Exception:
                time.sleep(0.2)
        raise RuntimeError(f"Failed to fetch slice key {s_key}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_slice, i, key): i for i, key in enumerate(all_slice_keys)}
        for f in concurrent.futures.as_completed(futures):
            i, b = f.result()
            slices[i] = b

    assembled = bytearray()
    for s in slices:
        if s:
            assembled.extend(s)
    full_file = bytes(assembled[:expected_size])

    calc_sha256 = hashlib.sha256(full_file).hexdigest()
    elapsed = time.time() - start_time
    print(f"\nReconstructed SHA-256 : {calc_sha256}")
    print(f"Expected SHA-256      : {expected_sha256}")

    if expected_sha256 and calc_sha256 != expected_sha256:
        print("❌ INTEGRITY CHECK FAILED: SHA-256 mismatch!")
        sys.exit(1)

    print("🎉 INTEGRITY CHECK PASSED: 100% SHA-256 match!")
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "wb") as f:
        f.write(full_file)
    rate_mb = (len(full_file) / 1024 / 1024) / max(0.001, elapsed)
    print(f"💾 File saved successfully to: {output_path}")
    print(f"⏱️ Total Retrieval Time (Parallel Baseline): {elapsed:.3f}s ({rate_mb:.2f} MB/s)")
    print("=" * 80)


def retrieve_file_from_manifest(manifest_path, output_path, publisher_url, workers=32, retrieval_mode="auto"):
    print("=" * 80)
    print("CDA NETWORK - FILE RETRIEVAL & INTEGRITY VERIFICATION (MANIFEST MODE)")
    print(f"Manifest Path  : {manifest_path}")
    print(f"Output Target  : {output_path}")
    print(f"Publisher API  : {publisher_url}")
    print(f"Retrieval Mode : {retrieval_mode.upper()}")
    print("=" * 80)

    if not os.path.isfile(manifest_path):
        raise FileNotFoundError(f"Manifest file not found: {manifest_path}")

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    file_id = manifest["file_id"]
    filename = manifest.get("filename", "retrieved_file.bin")
    expected_size = manifest["file_size"]
    expected_sha256 = manifest["file_sha256"]
    chunks_meta = manifest["chunks"]

    print(f"\n📂 File ID      : {file_id}")
    print(f"📄 Original Name: {filename}")
    print(f"📦 Expected Size: {expected_size} bytes ({expected_size / 1024 / 1024:.2f} MB)")
    print(f"🧩 Total Chunks : {len(chunks_meta)}")
    print(f"🔒 Expected Hash: {expected_sha256}")
    print("-" * 80)

    reconstructed_chunks = [None] * len(chunks_meta)
    start_time = time.time()

    for idx, c_meta in enumerate(chunks_meta):
        c_idx = c_meta["chunk_idx"]
        c_height = c_meta["height"]
        c_size = c_meta["size"]
        c_sha256 = c_meta["sha256"]
        tx_keys = c_meta.get("tx_keys")
        if not tx_keys:
            key_prefix = c_meta.get("key_prefix", f"{file_id}_{c_idx:03d}")
            slice_count = c_meta.get("slice_count") or c_meta.get("tx_count", 0)
            tx_keys = [f"{key_prefix}_{s_idx:04d}" for s_idx in range(slice_count)]

        print(f"\n[Retrieving Chunk #{c_idx:03d} / {len(chunks_meta)}] Size: {c_size} bytes | Height: #{c_height}...")

        chunk_bytes = None
        t_start_chunk = time.time()

        # Batch query attempt via POST /tx/batch
        if chunk_bytes is None and retrieval_mode in ["auto", "batch"]:
            try:
                batch_size = 500
                chunk_slice_data = {}
                batch_err = False
                for bi in range(0, len(tx_keys), batch_size):
                    sub_keys = tx_keys[bi:bi + batch_size]
                    req_data = json.dumps({"keys": sub_keys}).encode('utf-8')
                    batch_req = urllib.request.Request(
                        f"{publisher_url}/tx/batch",
                        data=req_data,
                        headers={"Content-Type": "application/json"}
                    )
                    with urllib.request.urlopen(batch_req, timeout=15) as b_resp:
                        if b_resp.status == 200:
                            b_data = json.loads(b_resp.read().decode('utf-8'))
                            if b_data.get("status") == "success":
                                res_map = b_data.get("results", {})
                                for k, v in res_map.items():
                                    if "=" in v:
                                        v = v.split("=", 1)[1]
                                    chunk_slice_data[k] = bytes.fromhex(v)
                            else:
                                batch_err = True
                                break
                        else:
                            batch_err = True
                            break
                if not batch_err and len(chunk_slice_data) == len(tx_keys):
                    assembled = bytearray()
                    for k in tx_keys:
                        assembled.extend(chunk_slice_data[k])
                    raw_full = bytes(assembled[:c_size])
                    if not c_sha256 or hashlib.sha256(raw_full).hexdigest() == c_sha256:
                        chunk_bytes = raw_full
                        dur_c = time.time() - t_start_chunk
                        rate_c = (len(chunk_bytes) / 1024 / 1024) / max(0.001, dur_c)
                        print(f"  🎉 Chunk #{c_idx:03d} retrieved via Batch Query API in {dur_c:.3f}s ({rate_c:.2f} MB/s)")
            except Exception:
                pass

        # Fallback / parallel query
        if chunk_bytes is None:
            slices = [None] * len(tx_keys)

            def fetch_slice(s_idx, s_key):
                for retry in range(3):
                    try:
                        hex_val = fetch_single_tx_by_key(publisher_url, s_key, timeout=10)
                        return s_idx, bytes.fromhex(hex_val)
                    except Exception:
                        time.sleep(0.3)
                raise RuntimeError(f"Failed to fetch slice key {s_key}")

            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(fetch_slice, i, key): i
                    for i, key in enumerate(tx_keys)
                }
                for f in concurrent.futures.as_completed(futures):
                    i, b = f.result()
                    slices[i] = b

            assembled = bytearray()
            for s in slices:
                if s:
                    assembled.extend(s)
            assembled = assembled[:c_size]

            calc_chunk_sha256 = hashlib.sha256(assembled).hexdigest()
            if calc_chunk_sha256 != c_sha256:
                raise ValueError(f"Chunk #{c_idx} SHA-256 mismatch! Expected {c_sha256}, got {calc_chunk_sha256}")

            chunk_bytes = bytes(assembled)
            dur_c = time.time() - t_start_chunk
            rate_c = (len(chunk_bytes) / 1024 / 1024) / max(0.001, dur_c)
            print(f"  ✅ Reconstructed via Two-Tier /tx?key= query in {dur_c:.3f}s ({rate_c:.2f} MB/s)")

        reconstructed_chunks[c_idx] = chunk_bytes

    full_file = bytearray()
    for c in reconstructed_chunks:
        full_file.extend(c)

    final_size = len(full_file)
    final_sha256 = hashlib.sha256(full_file).hexdigest()
    elapsed = time.time() - start_time

    print("\n" + "=" * 80)
    print("VERIFICATION & SUMMARY")
    print(f"Total Reconstructed Bytes : {final_size} / {expected_size}")
    print(f"Reconstructed SHA-256     : {final_sha256}")
    print(f"Expected SHA-256          : {expected_sha256}")

    if final_sha256 != expected_sha256 or final_size != expected_size:
        print("❌ INTEGRITY CHECK FAILED: File hash does not match original!")
        sys.exit(1)

    print("🎉 INTEGRITY CHECK PASSED: 100% SHA-256 exact match!")

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "wb") as f:
        f.write(full_file)

    rate_mb = (final_size / 1024 / 1024) / max(0.001, elapsed)
    print(f"💾 File saved successfully to: {output_path}")
    print(f"⏱️ Total Retrieval Time      : {elapsed:.3f}s ({rate_mb:.2f} MB/s)")
    print("=" * 80)


# ------------------------------------------------------------------------------
# UPLOAD / SUBMISSION MODULE
# ------------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="CDA Network - File Ingestion, Chunking & Verification Bot")
    parser.add_argument("--file", type=str, default="", help="Path to existing file to upload and store")
    parser.add_argument("--size", type=str, default="2MB", help="Synthetic file size if no --file is given (e.g. '2MB', '1.5M', '500KB')")
    parser.add_argument("--chunk-size", type=str, default="500KB", help="Target chunk size per block (default: 500KB, ~K=32)")
    parser.add_argument("--slice-size", type=int, default=500, help="Payload bytes per transaction slice (default: 500 bytes)")
    parser.add_argument("--namespace", type=str, default="cda-default", help="Target namespace (default: 'cda-default')")
    parser.add_argument("--blob-id", type=str, default="", help="Optional unique blob ID (defaults to auto-generated)")
    parser.add_argument("--uri", type=str, default="", help="CDA URI for retrieval (e.g. 'cda://namespace/blob_id')")
    parser.add_argument("--list-blobs", action="store_true", default=False, help="List all registered blobs in namespace")
    parser.add_argument("--manifest", type=str, default="", help="Path to save or read manifest JSON file")
    parser.add_argument("--retrieve", action="store_true", default=False, help="Run in retrieval mode: reconstruct file from URI or manifest")
    parser.add_argument("--retrieval-mode", type=str, default="auto", choices=["auto", "direct", "batch", "parallel"], help="Retrieval mode: 'auto' (direct->batch->parallel), 'direct' (streaming API), 'batch' (POST /tx/batch), 'parallel' (single slice queries)")
    parser.add_argument("--output", type=str, default="", help="Target output file path for --retrieve mode")
    parser.add_argument("--cometbft", type=str, default="http://localhost:26657", help="CometBFT RPC endpoint")
    parser.add_argument("--publisher", type=str, default="http://localhost:8080", help="CDA Publisher API endpoint")
    parser.add_argument("--bootstrap", type=str, default="http://localhost:9200", help="CDA Bootstrap API endpoint")
    parser.add_argument("--workers", type=int, default=32, help="Concurrency workers for broadcasting (default: 32)")
    parser.add_argument("--native-blob", action=argparse.BooleanOptionalAction, default=True, help="Enable Native Blob Transaction mode (default: true)")
    parser.add_argument("--wait-cda", action=argparse.BooleanOptionalAction, default=True, help="Wait for CDA BlockReady in the background (default: true)")
    parser.add_argument("--max-in-flight", type=int, default=4, help="Maximum in-flight blocks in CDA pipeline (default: 4)")
    args = parser.parse_args()

    # 1. Dispatch: List blobs in namespace
    if args.list_blobs:
        list_blobs_in_namespace(args.namespace, args.publisher, args.cometbft)
        return

    # 2. Dispatch: Retrieval mode
    if args.retrieve:
        if args.uri:
            ns, b_id = parse_cda_uri(args.uri)
            retrieve_file_stateless(ns, b_id, args.output, args.publisher, args.cometbft, workers=args.workers, retrieval_mode=args.retrieval_mode)
            return
        elif args.namespace and args.blob_id:
            retrieve_file_stateless(args.namespace, args.blob_id, args.output, args.publisher, args.cometbft, workers=args.workers, retrieval_mode=args.retrieval_mode)
            return
        elif args.manifest:
            out_path = args.output
            if not out_path:
                out_path = "retrieved_" + os.path.basename(args.manifest).replace(".manifest.json", "")
            retrieve_file_from_manifest(args.manifest, out_path, args.publisher, workers=args.workers, retrieval_mode=args.retrieval_mode)
            return
        else:
            print("❌ Error: --retrieve requires either --uri cda://<ns>/<id>, --namespace & --blob-id, or --manifest <file.json>")
            sys.exit(1)

    # 3. Ingestion mode
    chunk_size_bytes = parse_size_str(args.chunk_size)
    slice_size_bytes = args.slice_size
    namespace = args.namespace.strip()

    if args.file:
        if not os.path.isfile(args.file):
            print(f"❌ Input file not found: {args.file}")
            sys.exit(1)
        file_name = os.path.basename(args.file)
        with open(args.file, "rb") as f:
            file_bytes = f.read()
        print(f"📖 Loaded file '{file_name}' from disk ({len(file_bytes)} bytes).")
    else:
        total_size = parse_size_str(args.size)
        file_name = f"synthetic_data_{total_size // 1024}KB.bin"
        print(f"🎲 Generating synthetic pseudo-random data file of {total_size} bytes ({total_size / 1024 / 1024:.2f} MB)...")
        file_bytes = os.urandom(total_size)

    blob_id = args.blob_id.strip() if args.blob_id else f"b_{uuid.uuid4().hex[:8]}"
    file_sha256 = hashlib.sha256(file_bytes).hexdigest()
    total_file_size = len(file_bytes)
    total_chunks = math.ceil(total_file_size / chunk_size_bytes)
    total_slices = math.ceil(total_file_size / slice_size_bytes)

    manifest_output_path = args.manifest
    if not manifest_output_path:
        manifest_output_path = f"{file_name}.manifest.json"

    print("=" * 80)
    print("CDA NETWORK - FILE PIPELINE & NAMESPACE INGESTION BOT")
    print(f"Namespace       : {namespace}")
    print(f"Blob ID         : {blob_id}")
    print(f"Target URI      : cda://{namespace}/{blob_id}")
    print(f"File Name       : {file_name}")
    print(f"Total Size      : {total_file_size} bytes ({total_file_size / 1024 / 1024:.2f} MB)")
    print(f"SHA-256         : {file_sha256}")
    print(f"Chunk Size      : {chunk_size_bytes} bytes (~{chunk_size_bytes / 1024:.1f} KB / block)")
    print(f"Total Chunks    : {total_chunks}")
    print(f"Total Slices    : {total_slices}")
    print(f"Slice Size      : {slice_size_bytes} bytes / tx")
    print(f"CometBFT RPC    : {args.cometbft}")
    print(f"CDA Publisher   : {args.publisher}")
    print(f"CDA Bootstrap   : {args.bootstrap}")
    print(f"Backup Manifest : {manifest_output_path}")
    print("=" * 80)

    # Health check
    try:
        status = http_get_json(f"{args.cometbft}/status", timeout=5)
        sync_info = status.get("result", {}).get("sync_info", {})
        node_info = status.get("result", {}).get("node_info", {})
        print(f"✅ CometBFT connected | Node: {node_info.get('moniker')} | Current Height: {sync_info.get('latest_block_height')}\n")
    except Exception as e:
        print(f"❌ Cannot connect to CometBFT at {args.cometbft}: {e}")
        sys.exit(1)

    cda_queue = queue.Queue()
    in_flight_cond = threading.Condition()
    in_flight_count = 0
    cda_completed_records = {}

    def cda_monitor_worker():
        nonlocal in_flight_count
        while True:
            item = cda_queue.get()
            if item is None:
                cda_queue.task_done()
                break
            b_id, t_start, exp_k, c_idx, h = item
            ready, dur_cda, ready_k = wait_for_block_ready(args.bootstrap, b_id, publisher_url=args.publisher, timeout=120)
            info = fetch_publisher_block_info(args.publisher, b_id, max_retries=5)
            k_fin = ready_k if ready_k > 0 else (info['k'] if info else exp_k)
            commits_root = info['commits_root'] if info else ""

            if ready:
                print(f"  🎉 [CDA BlockReady] Chunk #{c_idx:03d} ({b_id}, Height #{h}) ready in {dur_cda:.3f}s | K={k_fin} | Root: {commits_root[:16]}...")
                cda_completed_records[c_idx] = {
                    "ready": True,
                    "duration": dur_cda,
                    "k": k_fin,
                    "commits_root": commits_root
                }
            else:
                print(f"  ⚠️ Timeout waiting for CDA BlockReady for Chunk #{c_idx:03d} ({b_id}) after {dur_cda:.3f}s")
                cda_completed_records[c_idx] = {
                    "ready": False,
                    "duration": dur_cda,
                    "k": k_fin,
                    "commits_root": commits_root
                }

            with in_flight_cond:
                in_flight_count -= 1
                in_flight_cond.notify_all()
            cda_queue.task_done()

    if args.wait_cda:
        mon_thread = threading.Thread(target=cda_monitor_worker, daemon=True)
        mon_thread.start()

    manifest_chunks = []
    start_time = time.time()

    # Build Head Manifest
    on_chain_manifest = {
        "version": 1,
        "namespace": namespace,
        "blob_id": blob_id,
        "filename": file_name,
        "total_size": total_file_size,
        "sha256": file_sha256,
        "total_chunks": total_chunks,
        "chunk_size": chunk_size_bytes,
        "slice_size": slice_size_bytes,
        "total_slices": total_slices,
        "created_at": int(time.time()),
    }
    manifest_raw = json.dumps(on_chain_manifest, separators=(',', ':')).encode('utf-8')
    manifest_tx_str = f"cda/{namespace}/{blob_id}/meta={manifest_raw.hex()}"

    try:
        if args.native_blob:
            print(f"\n🚀 [Native Blob Mode] Uploading {total_file_size / 1024:.1f} KB as Native Blob Transaction(s)...")
            txs = []
            tx_keys = []
            # 1. Manifest
            txs.append(manifest_tx_str.encode('utf-8'))
            tx_keys.append(f"cda/{namespace}/{blob_id}/meta")

            # 2. Blobs / Parts
            if total_chunks <= 1:
                blob_key = f"cda/{namespace}/{blob_id}/blob"
                blob_val = file_bytes.hex()
                tx_str = f"{blob_key}={blob_val}"
                txs.append(tx_str.encode('utf-8'))
                tx_keys.append(blob_key)
                print(f"  -> Generated 1 Single BlobTx: {blob_key} ({len(file_bytes) / 1024:.1f} KB)")
            else:
                for chunk_idx in range(total_chunks):
                    offset = chunk_idx * chunk_size_bytes
                    chunk_data = file_bytes[offset:offset + chunk_size_bytes]
                    part_key = f"cda/{namespace}/{blob_id}/part/{chunk_idx:03d}"
                    part_val = chunk_data.hex()
                    tx_str = f"{part_key}={part_val}"
                    txs.append(tx_str.encode('utf-8'))
                    tx_keys.append(part_key)
                print(f"  -> Generated {total_chunks} Part BlobTx(s): {chunk_size_bytes / 1024:.1f} KB each")

            t_blob_start = time.time()
            first_hash, last_hash, all_hashes = broadcast_tx_batch(args.cometbft, txs, max_workers=args.workers, timeout=30)
            height = wait_for_tx_commit(args.cometbft, last_hash, timeout=60)
            dur_commit = time.time() - t_blob_start
            block_id = f"block-{height}"
            print(f"  ✅ [Native Blob Committed] Height #{height} ({block_id}) | {len(txs)} txs | Latency: {dur_commit:.3f}s")

            if args.wait_cda:
                with in_flight_cond:
                    in_flight_count += 1
                cda_queue.put((block_id, t_blob_start, 32, 0, height))

            manifest_chunks.append({
                "chunk_idx": 0,
                "offset": 0,
                "size": total_file_size,
                "sha256": file_sha256,
                "height": height,
                "block_id": block_id,
                "tx_count": len(txs),
                "slice_count": len(txs),
                "slice_size": total_file_size,
                "key_prefix": f"cda/{namespace}/{blob_id}",
                "tx_keys": tx_keys,
                "first_key": tx_keys[0],
                "last_key": tx_keys[-1],
                "first_tx_hash": first_hash,
                "last_tx_hash": last_hash,
                "cda_commits_root": "",
                "cda_k": 32,
                "cda_ready": False
            })
        else:
            for chunk_idx in range(total_chunks):
                offset = chunk_idx * chunk_size_bytes
                chunk_data = file_bytes[offset:offset + chunk_size_bytes]
                actual_chunk_size = len(chunk_data)
                chunk_sha256 = hashlib.sha256(chunk_data).hexdigest()

                # Split chunk into slices
                num_slices = math.ceil(actual_chunk_size / slice_size_bytes)
                txs = []
                tx_keys = []

                # Prepend Head Manifest to Chunk 0
                if chunk_idx == 0:
                    txs.append(manifest_tx_str.encode('utf-8'))
                    tx_keys.append(f"cda/{namespace}/{blob_id}/meta")

                for s_idx in range(num_slices):
                    s_offset = s_idx * slice_size_bytes
                    slice_bytes = chunk_data[s_offset:s_offset + slice_size_bytes]
                    key = f"cda/{namespace}/{blob_id}/s/{chunk_idx:03d}_{s_idx:04d}"
                    val = slice_bytes.hex()
                    tx_str = f"{key}={val}"
                    txs.append(tx_str.encode('utf-8'))
                    tx_keys.append(key)

                print(f"[Chunk #{chunk_idx:03d} / {total_chunks:03d}] Offset {offset:8d}..{offset + actual_chunk_size:8d} ({actual_chunk_size / 1024:.1f} KB) | {len(txs)} txs...")

                t_chunk_start = time.time()
                first_hash, last_hash, all_hashes = broadcast_tx_batch(args.cometbft, txs, max_workers=args.workers, timeout=30)

                # Wait for commit into CometBFT
                height = wait_for_tx_commit(args.cometbft, first_hash, timeout=60)
                block_id = f"block-{height}"
                dur_commit = time.time() - t_chunk_start
                print(f"  ✅ Committed into CometBFT Height #{height} ({block_id}) | Latency: {dur_commit:.3f}s")

                # Queue for background CDA tracking
                if args.wait_cda:
                    with in_flight_cond:
                        if args.max_in_flight > 0:
                            while in_flight_count >= args.max_in_flight:
                                in_flight_cond.wait(timeout=0.2)
                        in_flight_count += 1
                    cda_queue.put((block_id, t_chunk_start, 32, chunk_idx, height))

                key_prefix = f"cda/{namespace}/{blob_id}/s/{chunk_idx:03d}"
                manifest_chunks.append({
                    "chunk_idx": chunk_idx,
                    "offset": offset,
                    "size": actual_chunk_size,
                    "sha256": chunk_sha256,
                    "height": height,
                    "block_id": block_id,
                    "tx_count": len(txs),
                    "slice_count": num_slices,
                    "slice_size": slice_size_bytes,
                    "key_prefix": key_prefix,
                    "tx_keys": tx_keys,
                    "first_key": tx_keys[0] if tx_keys else "",
                    "last_key": tx_keys[-1] if tx_keys else "",
                    "first_tx_hash": first_hash,
                    "last_tx_hash": last_hash,
                    "cda_commits_root": "",
                    "cda_k": 32,
                    "cda_ready": False
                })

    except KeyboardInterrupt:
        print("\nUpload interrupted by user.")

    if args.wait_cda and not cda_queue.empty():
        print(f"\n[Upload Complete] Waiting for remaining in-flight CDA verification...")
        cda_queue.join()

    # Merge background CDA verification results
    for m_chunk in manifest_chunks:
        c_idx = m_chunk["chunk_idx"]
        if c_idx in cda_completed_records:
            rec = cda_completed_records[c_idx]
            m_chunk["cda_ready"] = rec.get("ready", False)
            m_chunk["cda_commits_root"] = rec.get("commits_root", "")
            m_chunk["cda_k"] = rec.get("k", 32)
        else:
            info = fetch_publisher_block_info(args.publisher, m_chunk["block_id"], max_retries=3)
            if info:
                m_chunk["cda_commits_root"] = info.get("commits_root", "")
                m_chunk["cda_k"] = info.get("k", 32)

    # Build and write final backup manifest
    manifest_data = {
        "file_id": blob_id,
        "namespace": namespace,
        "uri": f"cda://{namespace}/{blob_id}",
        "filename": file_name,
        "file_size": total_file_size,
        "file_sha256": file_sha256,
        "chunk_size": chunk_size_bytes,
        "slice_size": slice_size_bytes,
        "total_chunks": len(manifest_chunks),
        "total_slices": total_slices,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "cometbft_rpc": args.cometbft,
        "publisher_url": args.publisher,
        "bootstrap_url": args.bootstrap,
        "chunks": manifest_chunks
    }

    with open(manifest_output_path, "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=2)

    total_time = time.time() - start_time
    throughput_mb = (total_file_size / 1024 / 1024) / max(0.001, total_time)

    print("\n" + "=" * 80)
    print("FILE INGESTION & SUBMISSION FINISHED")
    print(f"Chunks Uploaded   : {len(manifest_chunks)} / {total_chunks}")
    print(f"Total Data Stored : {total_file_size} bytes ({total_file_size / 1024 / 1024:.2f} MB)")
    print(f"Original SHA-256  : {file_sha256}")
    print(f"Effective Speed   : {throughput_mb:.2f} MB/s (Total time: {total_time:.2f}s)")
    print(f"🎉 Registered URI : cda://{namespace}/{blob_id}")
    print(f"📁 Backup Manifest: {manifest_output_path}")
    print("\n💡 To retrieve this file statelessly from anywhere (no JSON needed):")
    print(f"   python3 scripts/bots/file_pipeline_bot.py --retrieve --uri cda://{namespace}/{blob_id} --output restored_{file_name}")
    print("\n💡 To list all files in this namespace:")
    print(f"   python3 scripts/bots/file_pipeline_bot.py --list-blobs --namespace {namespace}")
    print("=" * 80)


if __name__ == "__main__":
    main()
