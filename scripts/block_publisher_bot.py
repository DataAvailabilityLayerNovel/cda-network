#!/usr/bin/env python3
"""
CDA Network Block Publisher Bot - Event-Driven Mode.

Listens to the Bootstrap node's SSE endpoint (/events/block-ready) and
publishes the next block only after receiving confirmation that the previous
block has been fully seeded by store nodes. Falls back to polling
/block-ready/latest and a fixed interval if SSE is unavailable.
"""
import argparse
import json
import random
import time
import urllib.request
import urllib.error


def generate_ods_data(k):
    """Generates k*k dummy cell field elements as 128-char hex strings."""
    return ['%0128x' % random.randint(1, 1_000_000) for _ in range(k * k)]


def publish_block(publish_url, block_id, k, timeout=60):
    """Publish a single block. Returns (success, latency_seconds)."""
    data = generate_ods_data(k)
    payload = {"block_id": block_id, "data": data}
    req_data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        publish_url,
        data=req_data,
        headers={'Content-Type': 'application/json'},
    )
    start = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp.read()
        return True, time.time() - start
    except Exception as e:
        return False, time.time() - start


def wait_for_block_ready_sse(sse_url, expected_block_id, timeout=120):
    """
    Subscribe to SSE stream and block until expected_block_id appears.
    Returns True on success, False on timeout or connection error.
    """
    deadline = time.time() + timeout
    try:
        req = urllib.request.Request(sse_url, headers={'Accept': 'text/event-stream'})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            while time.time() < deadline:
                line = resp.readline()
                if not line:
                    break
                line = line.strip()
                if line.startswith(b"data:"):
                    raw = line[5:].strip()
                    try:
                        pl = json.loads(raw)
                        bid = pl.get("block_id", "")
                        h   = pl.get("height", 0)
                        print(f"  [SSE] BlockReady received: {bid} (height {h})")
                        if bid == expected_block_id:
                            return True
                    except Exception:
                        pass
    except Exception as e:
        print(f"  [SSE] Connection error: {e}")
    return False


def wait_for_block_ready_poll(latest_url, expected_block_id, poll_interval=2.0, timeout=60):
    """
    Poll /block-ready/latest until expected_block_id appears.
    Fallback when SSE is unavailable.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(latest_url, timeout=5) as resp:
                if resp.status == 204:
                    time.sleep(poll_interval)
                    continue
                pl = json.loads(resp.read())
                if pl.get("block_id") == expected_block_id:
                    print(f"  [Poll] BlockReady confirmed: {expected_block_id}")
                    return True
        except Exception:
            pass
        time.sleep(poll_interval)
    return False


def main():
    parser = argparse.ArgumentParser(
        description="CDA Network Block Publisher Bot (Event-Driven)")
    parser.add_argument("--k", type=int, default=32,
                        help="ODS matrix dimension K (K x K cells)")
    parser.add_argument("--publisher", type=str, default="http://localhost:8080",
                        help="Publisher Node base URL")
    parser.add_argument("--bootstrap", type=str, default="http://localhost:9200",
                        help="Bootstrap Node base URL for BlockReady SSE events")
    parser.add_argument("--count", type=int, default=0,
                        help="Number of blocks to publish (0 = infinite)")
    parser.add_argument("--start-height", type=int, default=1,
                        help="Starting block height")
    parser.add_argument("--interval", type=float, default=0.0,
                        help="Fixed wait interval in seconds (0 = event-driven)")
    parser.add_argument("--ready-timeout", type=float, default=120.0,
                        help="Max seconds to wait for BlockReady before pushing next block anyway")
    args = parser.parse_args()

    publish_url = f"{args.publisher.rstrip('/')}/publish"
    sse_url     = f"{args.bootstrap.rstrip('/')}/events/block-ready"
    latest_url  = f"{args.bootstrap.rstrip('/')}/block-ready/latest"

    print(f"[*] CDA Block Publisher Bot — Event-Driven Mode")
    print(f"[*] Publisher : {publish_url}")
    print(f"[*] Bootstrap : {args.bootstrap}")
    print(f"[*] Matrix K  : {args.k}  (ODS cells: {args.k * args.k})")
    print(f"[*] Mode      : {'fixed interval ' + str(args.interval) + 's' if args.interval > 0 else 'event-driven (BlockReady SSE)'}")
    print(f"[*] Timeout   : {args.ready_timeout}s per block")
    if args.count > 0:
        print(f"[*] Will publish {args.count} blocks then exit.")
    else:
        print(f"[*] Infinite mode. Ctrl+C to stop.")

    height    = args.start_height
    published = 0

    while True:
        block_id = f"block-{height}"
        ts = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n[*] [{ts}] Publishing {block_id} (height {height})...")

        ok, latency = publish_block(publish_url, block_id, args.k)
        if not ok:
            print(f"[-] Failed to publish {block_id} ({latency:.3f}s) — retrying in 5s")
            time.sleep(5)
            continue

        print(f"[+] Published {block_id} in {latency:.3f}s")
        published += 1

        if args.count > 0 and published >= args.count:
            print(f"\n[*] Reached target of {args.count} blocks. Done.")
            break

        if args.interval > 0:
            # Fixed interval mode (legacy)
            print(f"[*] Waiting {args.interval}s before next block...")
            time.sleep(args.interval)
        else:
            # Event-driven: wait for BlockReady from bootstrap SSE or poll
            print(f"[*] Waiting for BlockReady signal for {block_id} (max {args.ready_timeout}s)...")
            ready = wait_for_block_ready_poll(latest_url, block_id, poll_interval=0.1, timeout=0.5)
            if not ready:
                ready = wait_for_block_ready_sse(sse_url, block_id, timeout=args.ready_timeout)
                if not ready:
                    print(f"  [!] SSE timeout/error — falling back to polling /block-ready/latest")
                    ready = wait_for_block_ready_poll(
                        latest_url, block_id,
                        timeout=max(10, args.ready_timeout / 3))
            if ready:
                print(f"[+] BlockReady confirmed for {block_id} — pushing next block.")
            else:
                print(f"[!] No BlockReady in {args.ready_timeout}s — pushing next block anyway.")

        height += 1


if __name__ == "__main__":
    main()
