# CDA Network — Web Dashboard

Single-page web dashboard for inspecting and interacting with the CDA Network.

---

## 1. Features

1. **Account Management**:
   - Generate ECDSA / Web Crypto key pairs locally in the browser with deterministic address generation (`cda1_...`).
   - Default active namespace to user account address or custom namespaces (`rollup-1`, `defi-state`, `nft-metadata`).
2. **Submit Blob Transaction**:
   - Drag-and-drop file upload or direct text/hex payload input.
   - Computes SHA-256 payload digest.
   - Encodes transactions into CDA wire protocol: `cda/<namespace>/<blob_id>/blob=<hex>`.
   - Broadcasts directly to CometBFT consensus via JSON-RPC `broadcast_tx_sync`.
3. **Namespace Catalog & Download**:
   - Query all blobs registered under a namespace.
   - One-click binary download via Publisher Node Fast Path (`GET /api/v1/namespaces/{ns}/blobs/{id}/download`), served directly from BadgerDB memory-mapped cache.
4. **Block Explorer & ODS TOC**:
   - Monitors real-time block height and active namespace counts.
   - Inspects block structure and Table of Contents (TOC) with ODS cell coordinates (`cell_offset`, `cell_count`).
5. **Node Settings**:
   - Configurable endpoints for CometBFT JSON-RPC (`http://<ip>:26657`) and CDA Publisher Node (`http://<ip>:8889`).

---

## 2. Usage

### Option 1: Embedded in Publisher Node (Recommended)
When the Publisher Node is running, access the dashboard at:
```
http://<server-ip>:8889/dashboard/
```

### Option 2: Standalone Static Server
```bash
python3 -m http.server 3000 --directory web
```
Then visit `http://localhost:3000`.

### Option 3: Direct File Open
Open [`web/index.html`](file:///home/ubuntu/cda-network/web/index.html) directly in any modern browser. All endpoints support full CORS.
