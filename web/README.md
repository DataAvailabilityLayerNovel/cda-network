# CDA Network — Web Dashboard

Single-page web dashboard for inspecting and interacting with the CDA Network.

---

## 1. Features

1. **Account Management**:
   - Generate genuine RFC 8032 Ed25519 key pairs locally in the browser with deterministic address derivation (`cda1_<first_16_hex_of_sha256(pubKey)>`).
   - Import existing 64-character hex private seeds or export account keypair JSON backups.
   - Default active namespace to user account address or custom namespaces (`rollup-1`, `defi-state`, `nft-metadata`).
   - Account-bound Namespace Protection: Transactions targeting `cda1_...` namespaces are signed with Ed25519 (`@<pubKeyHex>.<sigHex>`) and cryptographically verified by CometBFT `CheckTx`.
2. **Submit Blob Transaction**:
   - Drag-and-drop file upload or direct text/JSON payload input.
   - Computes SHA-256 payload digest and commits an on-chain Head Manifest (`cda/<namespace>/<blob_id>/meta`) preserving original filenames.
   - Automatic Multi-Part chunking (`cda/<namespace>/<blob_id>/part/<idx:03d>`) for large files (> 256 KB), or Single Native Blob (`cda/<namespace>/<blob_id>/blob`) for compact files.
   - Broadcasts directly to CometBFT consensus via JSON-RPC `broadcast_tx_sync` and tracks two-phase finality: Consensus Commit (`COMMITTED #H`) $\to$ 2D RS / KZG Custody Quorum (`DA VERIFIED #H`).
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
