#!/usr/bin/env python3
import argparse
import time
import json
import urllib.request
import urllib.error
import random

def generate_ods_data(k):
    # Generates k * k dummy cell field elements as 128-character hex strings
    data = []
    for i in range(k * k):
        val = random.randint(1, 1000000)
        data.append('%0128x' % val)
    return data

def main():
    parser = argparse.ArgumentParser(description="CDA Network Block Publisher Bot")
    parser.add_argument("--interval", type=float, default=30.0, help="Interval between publishes in seconds")
    parser.add_argument("--k", type=int, default=32, help="ODS matrix dimension K (K x K cells)")
    parser.add_argument("--publisher", type=str, default="http://localhost:8080", help="Publisher Node base URL")
    parser.add_argument("--count", type=int, default=0, help="Number of blocks to publish (0 for infinite)")
    parser.add_argument("--start-height", type=int, default=1, help="Starting block height")
    args = parser.parse_args()

    publish_url = f"{args.publisher.rstrip('/')}/publish"
    print(f"[*] Starting Block Publisher Bot.")
    print(f"[*] Publisher URL: {publish_url}")
    print(f"[*] Matrix parameter K: {args.k} (Total ODS cells: {args.k * args.k})")
    print(f"[*] Interval: {args.interval} seconds")
    print(f"[*] Starting height: {args.start_height}")
    if args.count > 0:
        print(f"[*] Will publish {args.count} blocks and then exit.")
    else:
        print(f"[*] Running in infinite loop. Press Ctrl+C to stop.")

    published_count = 0
    height = args.start_height
    while True:
        block_id = f"block-{height}"
        print(f"\n[*] [{time.strftime('%Y-%m-%d %H:%M:%S')}] Generating data for block {block_id} (Height: {height})...")
        data = generate_ods_data(args.k)
        payload = {
            "block_id": block_id,
            "data": data
        }
        
        req_data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(
            publish_url,
            data=req_data,
            headers={'Content-Type': 'application/json'}
        )

        start_time = time.time()
        success = False
        error_msg = ""
        try:
            with urllib.request.urlopen(req) as response:
                res_body = response.read().decode('utf-8')
                latency = time.time() - start_time
                print(f"[+] Successfully published block {block_id} (latency: {latency:.3f}s)")
                success = True
        except urllib.error.HTTPError as e:
            latency = time.time() - start_time
            error_msg = f"HTTP Error {e.code}: {e.read().decode('utf-8', errors='ignore')}"
        except urllib.error.URLError as e:
            latency = time.time() - start_time
            error_msg = f"Network Error: {e.reason}"
        except Exception as e:
            latency = time.time() - start_time
            error_msg = f"Unexpected Error: {e}"

        if not success:
            print(f"[-] Failed to publish block {block_id} (latency: {latency:.3f}s)")
            print(f"[-] Error details: {error_msg}")

        published_count += 1
        height += 1
        if args.count > 0 and published_count >= args.count:
            print(f"\n[*] Reached targeted count of {args.count} blocks. Exiting.")
            break

        time.sleep(args.interval)

if __name__ == "__main__":
    main()
