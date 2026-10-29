#!/usr/bin/env python3
"""
file_pipeline_bot.py - CDA Network File Ingestion, Chunking & Verification Bot.

Handles:
1. Ingestion: Reads a file from disk (--file) or generates synthetic data (--size).
2. Chunking: Splits the file into ~500 KB chunks, each formatted into key-value
   transaction slices for CometBFT and CDA Network.
3. Pipelined Submission: Submits chunks consecutively into CometBFT blocks while
   tracking CDA BlockReady and KZG commitments asynchronously.
4. Manifest Recording: Generates a comprehensive JSON manifest with all metadata
   (file hash, block heights, tx keys/hashes, KZG commits_root) needed for exact retrieval.
5. Verification & Retrieval (--retrieve): Downloads chunks via CDA Publisher API,
   reassembles the original file, and validates SHA-256 hash match.
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
    last_tx_hash = None
    first_tx_hash = None
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
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as resp:
                data = json.loads(resp.read().decode('utf-8'))
                res = data.get("result", {})
                h = res.get("height")
                if h:
                    return int(h)
        except Exception:
            pass
        time.sleep(poll_interval)
    raise TimeoutError(f"Tx {tx_hash} not committed within {timeout}s")


def fetch_publisher_block_info(publisher_url, block_id, max_retries=15, poll_interval=0.2):
    url = f"{publisher_url}/header/{block_id}"
    for _ in range(max_retries):
        try:
            data = http_get_json(url, timeout=3)
            if data and "column_comm" in data:
                n = len(data["column_comm"])
                k = n // 2
                return {
                    "k": k,
                    "n": n,
                    "commits_root": data.get("commits_root", ""),
                    "chain_id": data.get("chain_id", ""),
                }
        except Exception:
            pass
        time.sleep(poll_interval)
    return None


def wait_for_block_ready(bootstrap_url, expected_block_id, publisher_url=None, timeout=120, poll_interval=0.15):
    latest_url = f"{bootstrap_url}/block-ready/latest"
    start = time.time()
    deadline = start + timeout

    while time.time() < deadline:
        # 1. Exact match on Bootstrap /block-ready/latest
        try:
            with urllib.request.urlopen(latest_url, timeout=2) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode('utf-8'))
                    if data.get("block_id") == expected_block_id:
                        return True, time.time() - start, data.get("k", 0)
        except Exception:
            pass

        # 2. Cross-check with Publisher da_quorum_ready
        if publisher_url:
            try:
                b_url = f"{publisher_url}/block/{expected_block_id}"
                with urllib.request.urlopen(b_url, timeout=2) as resp:
                    if resp.status == 200:
                        binfo = json.loads(resp.read().decode('utf-8'))
                        if binfo.get("da_quorum_ready") is True:
                            return True, time.time() - start, binfo.get("k", 0)
            except Exception:
                pass

        time.sleep(poll_interval)
    return False, time.time() - start, 0


# ------------------------------------------------------------------------------
# RETRIEVE MODULE
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


def retrieve_file_from_manifest(manifest_path, output_path, publisher_url, workers=32):
    print("=" * 80)
    print("CDA NETWORK - FILE RETRIEVAL & INTEGRITY VERIFICATION")
    print(f"Manifest Path : {manifest_path}")
    print(f"Output Target : {output_path}")
    print(f"Publisher API : {publisher_url}")
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

        # 1. First attempt: Retrieve via /block/:height/data (ODS Matrix Reconstruction)
        chunk_bytes = None
        t_start_chunk = time.time()
        try:
            block_data_url = f"{publisher_url}/block/{c_height}/data"
            b_resp = http_get_json(block_data_url, timeout=20)
            if b_resp.get("status") == "success" and b_resp.get("cells"):
                raw_cell_blob = bytearray()
                for c_hex in b_resp["cells"]:
                    cb = bytes.fromhex(c_hex)
                    raw_cell_blob.extend(cb)

                # Search and extract slices from raw_cell_blob
                found_slices = {}
                for key in tx_keys:
                    key_bytes = key.encode('utf-8') + b"="
                    pos = raw_cell_blob.find(key_bytes)
                    if pos != -1:
                        val_start = pos + len(key_bytes)
                        # Find end of slice hex data (either next slice or non-hex byte)
                        # Slice size in hex is known from manifest
                        exp_slice_hex_len = (c_meta.get("slice_size", 500)) * 2
                        slice_hex = raw_cell_blob[val_start:val_start + exp_slice_hex_len].decode('ascii', errors='ignore')
                        try:
                            found_slices[key] = bytes.fromhex(slice_hex)
                        except ValueError:
                            pass

                if len(found_slices) == len(tx_keys):
                    assembled = bytearray()
                    for k in tx_keys:
                        assembled.extend(found_slices[k])
                    assembled = assembled[:c_size]
                    if hashlib.sha256(assembled).hexdigest() == c_sha256:
                        chunk_bytes = bytes(assembled)
                        print(f"  ✅ Reconstructed via CDA Block #{c_height} ODS Matrix in {time.time() - t_start_chunk:.3f}s")
        except Exception as e:
            # Fallback to key-value query
            pass

        # 2. Fallback attempt: Retrieve via Two-Tier /tx?key=... in parallel
        if chunk_bytes is None:
            print(f"  ℹ️ Falling back to parallel /tx?key=... query for {len(tx_keys)} slices...")
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
                assembled.extend(s)
            assembled = assembled[:c_size]

            calc_chunk_sha256 = hashlib.sha256(assembled).hexdigest()
            if calc_chunk_sha256 != c_sha256:
                raise ValueError(f"Chunk #{c_idx} SHA-256 mismatch! Expected {c_sha256}, got {calc_chunk_sha256}")

            chunk_bytes = bytes(assembled)
            print(f"  ✅ Reconstructed via Two-Tier /tx?key= query in {time.time() - t_start_chunk:.3f}s")

        reconstructed_chunks[c_idx] = chunk_bytes

    # Assemble entire file
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

    # Write output file
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "wb") as f:
        f.write(full_file)

    rate_mb = (final_size / 1024 / 1024) / max(0.001, elapsed)
    print(f"💾 File saved successfully to: {output_path}")
    print(f"⏱️ Total Retrieval Time      : {elapsed:.2f}s ({rate_mb:.2f} MB/s)")
    print("=" * 80)


# ------------------------------------------------------------------------------
# UPLOAD / SUBMIT MODULE
# ------------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="CDA Network - File Ingestion, Chunking & Verification Bot")
    parser.add_argument("--file", type=str, default="", help="Path to existing file to upload and store")
    parser.add_argument("--size", type=str, default="2MB", help="Synthetic file size if no --file is given (e.g. '2MB', '1.5M', '500KB')")
    parser.add_argument("--chunk-size", type=str, default="500KB", help="Target chunk size per block (default: 500KB, ~K=32)")
    parser.add_argument("--slice-size", type=int, default=500, help="Payload bytes per transaction slice (default: 500 bytes)")
    parser.add_argument("--manifest", type=str, default="", help="Path to save or read manifest JSON file")
    parser.add_argument("--retrieve", action="store_true", default=False, help="Run in retrieval mode: reconstruct file from manifest")
    parser.add_argument("--output", type=str, default="", help="Target output file path for --retrieve mode")
    parser.add_argument("--cometbft", type=str, default="http://localhost:26657", help="CometBFT RPC endpoint")
    parser.add_argument("--publisher", type=str, default="http://localhost:8080", help="CDA Publisher API endpoint")
    parser.add_argument("--bootstrap", type=str, default="http://localhost:9200", help="CDA Bootstrap API endpoint")
    parser.add_argument("--workers", type=int, default=32, help="Concurrency workers for broadcasting (default: 32)")
    parser.add_argument("--wait-cda", action=argparse.BooleanOptionalAction, default=True, help="Wait for CDA BlockReady in the background (default: true)")
    parser.add_argument("--max-in-flight", type=int, default=4, help="Maximum in-flight blocks in CDA pipeline (default: 4)")
    args = parser.parse_args()

    # Dispatch to retrieve mode if requested
    if args.retrieve:
        manifest_path = args.manifest
        if not manifest_path:
            print("❌ Error: --retrieve requires --manifest <path_to_manifest.json>")
            sys.exit(1)
        out_path = args.output
        if not out_path:
            out_path = "retrieved_" + os.path.basename(manifest_path).replace(".manifest.json", "")
        retrieve_file_from_manifest(manifest_path, out_path, args.publisher, workers=args.workers)
        return

    # Ingestion mode
    chunk_size_bytes = parse_size_str(args.chunk_size)
    slice_size_bytes = args.slice_size

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

    file_id = f"f_{uuid.uuid4().hex[:8]}"
    file_sha256 = hashlib.sha256(file_bytes).hexdigest()
    total_file_size = len(file_bytes)
    total_chunks = math.ceil(total_file_size / chunk_size_bytes)

    manifest_output_path = args.manifest
    if not manifest_output_path:
        manifest_output_path = f"{file_name}.manifest.json"

    print("=" * 80)
    print("CDA NETWORK - FILE PIPELINE & INGESTION BOT")
    print(f"File ID         : {file_id}")
    print(f"File Name       : {file_name}")
    print(f"Total Size      : {total_file_size} bytes ({total_file_size / 1024 / 1024:.2f} MB)")
    print(f"SHA-256         : {file_sha256}")
    print(f"Chunk Size      : {chunk_size_bytes} bytes (~{chunk_size_bytes / 1024:.1f} KB / block)")
    print(f"Total Chunks    : {total_chunks}")
    print(f"Slice Size      : {slice_size_bytes} bytes / tx")
    print(f"CometBFT RPC    : {args.cometbft}")
    print(f"CDA Publisher   : {args.publisher}")
    print(f"CDA Bootstrap   : {args.bootstrap}")
    print(f"Manifest File   : {manifest_output_path}")
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
            tot_dur = time.time() - t_start
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

    try:
        for chunk_idx in range(total_chunks):
            offset = chunk_idx * chunk_size_bytes
            chunk_data = file_bytes[offset:offset + chunk_size_bytes]
            actual_chunk_size = len(chunk_data)
            chunk_sha256 = hashlib.sha256(chunk_data).hexdigest()

            # Split chunk into slices
            num_slices = math.ceil(actual_chunk_size / slice_size_bytes)
            txs = []
            tx_keys = []
            for s_idx in range(num_slices):
                s_offset = s_idx * slice_size_bytes
                slice_bytes = chunk_data[s_offset:s_offset + slice_size_bytes]
                key = f"{file_id}_{chunk_idx:03d}_{s_idx:04d}"
                val = slice_bytes.hex()
                tx_str = f"{key}={val}"
                txs.append(tx_str.encode('utf-8'))
                tx_keys.append(key)

            print(f"[Chunk #{chunk_idx:03d} / {total_chunks:03d}] Offset {offset:8d}..{offset + actual_chunk_size:8d} ({actual_chunk_size / 1024:.1f} KB) | {num_slices} tx slices...")

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

            key_prefix = f"{file_id}_{chunk_idx:03d}"
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
                "key_pattern": f"{key_prefix}_{{seq:04d}}",
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

    # Merge background CDA verification results into manifest chunks
    for m_chunk in manifest_chunks:
        c_idx = m_chunk["chunk_idx"]
        if c_idx in cda_completed_records:
            rec = cda_completed_records[c_idx]
            m_chunk["cda_ready"] = rec.get("ready", False)
            m_chunk["cda_commits_root"] = rec.get("commits_root", "")
            m_chunk["cda_k"] = rec.get("k", 32)
        else:
            # Query publisher header directly if not caught
            info = fetch_publisher_block_info(args.publisher, m_chunk["block_id"], max_retries=3)
            if info:
                m_chunk["cda_commits_root"] = info.get("commits_root", "")
                m_chunk["cda_k"] = info.get("k", 32)

    # Build and write final manifest
    manifest_data = {
        "file_id": file_id,
        "filename": file_name,
        "file_size": total_file_size,
        "file_sha256": file_sha256,
        "chunk_size": chunk_size_bytes,
        "slice_size": slice_size_bytes,
        "total_chunks": len(manifest_chunks),
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
    print(f"📁 Manifest Saved : {manifest_output_path}")
    print("\n💡 To retrieve and verify this file at any time, run:")
    print(f"   python3 scripts/bots/file_pipeline_bot.py --retrieve --manifest {manifest_output_path} --output restored_{file_name}")
    print("=" * 80)


if __name__ == "__main__":
    main()
