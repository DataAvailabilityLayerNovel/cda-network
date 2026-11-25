#!/bin/bash
set -e

# ==============================================================================
# CDA Network CLI Node & DAS Controller (scripts/cda_node_ctl.sh)
# ==============================================================================
# Commands:
#   ./scripts/cda_node_ctl.sh status [block_id]
#   ./scripts/cda_node_ctl.sh join-store [auto|0..7] [col]
#   ./scripts/cda_node_ctl.sh leave-store <port>
#   ./scripts/cda_node_ctl.sh join-light
#   ./scripts/cda_node_ctl.sh leave-light <port>
#   ./scripts/cda_node_ctl.sh das <block_id_or_height> [samples] [light_port]
#   ./scripts/cda_node_ctl.sh das-cell <block_id_or_height> <row> <col> [light_port]
#   ./scripts/cda_node_ctl.sh rep-status
#   ./scripts/cda_node_ctl.sh rep-sim <port_or_peer_or_all> <block_completed|das_served|missed_block|fetch_timeout|ungraceful_drop|byzantine_fault|manual_reset> [count] [block_id]
#   ./scripts/cda_node_ctl.sh rep-reset [port_or_peer_or_all]
# ==============================================================================

DASHBOARD_API="${DASHBOARD_API:-http://127.0.0.1:8889}"
CMD="${1:-status}"

case "$CMD" in
  status)
    BLOCK="${2:-block-5}"
    curl -s "${DASHBOARD_API}/api/network-matrix?block_id=${BLOCK}" | python3 -c "
import sys, json
d = json.load(sys.stdin)
if d.get('status') != 'success':
    print('Error:', d)
    sys.exit(1)
print(f'=== CDA 2D Network Matrix ({d[\"net_rows\"]}x{d[\"net_cols\"]}, Active Stores in Col 0: {d[\"active_peers_in_col\"]}, Light Nodes: {len(d[\"light_nodes\"])}) ===')
for c in d.get('cells', []):
    r = c['net_row']
    m = c['native_count']
    pool = [f\"{p.get('peer_id','')[:10]}..(R{p.get('row')})\" for p in c.get('custody_pool', [])]
    print(f'  Cell [Row {r}, Col 0] | Native M={m} | Custody Pool ({len(pool)}): {\", \".join(pool)}')
print('\nActive Store Nodes:')
for s in d.get('store_nodes', []):
    tag = 'SPAWNED' if s.get('is_spawned') else 'CLUSTER'
    print(f'  - Port :{s.get(\"port\")} | Row {s.get(\"net_row\")} | Trust={s.get(\"trust_score\",60.0):.1f} [{s.get(\"trust_tier\",\"STANDARD\")}] | CustodyRows={s.get(\"custody_net_rows\")} | [{tag}] {s.get(\"peer_id\")}')
print('\nActive Light Nodes:')
for l in d.get('light_nodes', []):
    tag = 'SPAWNED' if l.get('is_spawned') else 'CLUSTER'
    print(f'  - Port :{l.get(\"port\")} | [{tag}] {l.get(\"peer_id\")}')
"
    ;;

  join-store)
    ROW_ARG="${2:-auto}"
    COL_ARG="${3:-0}"
    if [ "$ROW_ARG" = "auto" ]; then
      ROW_VAL=-1
    else
      ROW_VAL="$ROW_ARG"
    fi
    curl -s -X POST -H "Content-Type: application/json" \
      -d "{\"row\": ${ROW_VAL}, \"col\": ${COL_ARG}}" \
      "${DASHBOARD_API}/api/nodes/store/spawn" | python3 -m json.tool
    ;;

  leave-store)
    PORT_ARG="$2"
    if [ -z "$PORT_ARG" ]; then
      echo "Usage: $0 leave-store <port>"
      exit 1
    fi
    curl -s -X POST -H "Content-Type: application/json" \
      -d "{\"port\": ${PORT_ARG}}" \
      "${DASHBOARD_API}/api/nodes/store/stop" | python3 -m json.tool
    ;;

  join-light)
    curl -s -X POST -H "Content-Type: application/json" \
      -d "{}" \
      "${DASHBOARD_API}/api/nodes/light/spawn" | python3 -m json.tool
    ;;

  leave-light)
    PORT_ARG="$2"
    if [ -z "$PORT_ARG" ]; then
      echo "Usage: $0 leave-light <port>"
      exit 1
    fi
    curl -s -X POST -H "Content-Type: application/json" \
      -d "{\"port\": ${PORT_ARG}}" \
      "${DASHBOARD_API}/api/nodes/light/stop" | python3 -m json.tool
    ;;

  das)
    BLOCK_ARG="${2:-5}"
    SAMPLES_ARG="${3:-4}"
    LIGHT_PORT="${4:-9401}"
    curl -s -X POST -H "Content-Type: application/json" \
      -d "{\"port\": ${LIGHT_PORT}, \"block_id\": \"${BLOCK_ARG}\", \"samples\": ${SAMPLES_ARG}}" \
      "${DASHBOARD_API}/api/nodes/light/sample" | python3 -m json.tool
    ;;

  das-cell)
    BLOCK_ARG="$2"
    ROW_ARG="$3"
    COL_ARG="$4"
    LIGHT_PORT="${5:-9401}"
    if [ -z "$BLOCK_ARG" ] || [ -z "$ROW_ARG" ] || [ -z "$COL_ARG" ]; then
      echo "Usage: $0 das-cell <block_id_or_height> <row> <col> [light_port]"
      exit 1
    fi
    curl -s -X POST -H "Content-Type: application/json" \
      -d "{\"port\": ${LIGHT_PORT}, \"block_id\": \"${BLOCK_ARG}\", \"row\": ${ROW_ARG}, \"col\": ${COL_ARG}}" \
      "${DASHBOARD_API}/api/nodes/light/sample" | python3 -m json.tool
    ;;

  rep-status)
    curl -s "${DASHBOARD_API}/api/network-matrix?block_id=block-5" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('=== CDA Store Node Trust & Reputation Leaderboard ===')
print(f'{\"Port\":<7} {\"Row\":<5} {\"Score\":<8} {\"Tier\":<13} {\"Completed\":<11} {\"Missed\":<8} {\"Forgiven\":<10} {\"LastEvent\":<17} {\"PeerID\"}')
print('-' * 105)
for s in d.get('store_nodes', []):
    p = f':{s.get(\"port\")}' if s.get('port') else '-'
    print(f'{p:<7} {s.get(\"net_row\",0):<5} {s.get(\"trust_score\",60.0):<8.1f} {s.get(\"trust_tier\",\"STANDARD\"):<13} {s.get(\"blocks_completed\",0):<11} {s.get(\"blocks_missed\",0):<8} {s.get(\"straggler_forgiven\",0):<10} {s.get(\"last_event\",\"joined\"):<17} {s.get(\"peer_id\",\"\")}')
"
    ;;

  rep-sim)
    TARGET_ARG="$2"
    EVENT_ARG="$3"
    COUNT_ARG="${4:-1}"
    BLOCK_ARG="${5:-}"
    if [ -z "$TARGET_ARG" ] || [ -z "$EVENT_ARG" ]; then
      echo "Usage: $0 rep-sim <port_or_peer_or_all> <block_completed|das_served|missed_block|fetch_timeout|ungraceful_drop|byzantine_fault|manual_reset> [count] [block_id]"
      exit 1
    fi
    curl -s -X POST -H "Content-Type: application/json" \
      -d "{\"target\": \"${TARGET_ARG}\", \"event\": \"${EVENT_ARG}\", \"count\": ${COUNT_ARG}, \"block_id\": \"${BLOCK_ARG}\"}" \
      "${DASHBOARD_API}/api/reputation/event" | python3 -c "
import sys, json
d = json.load(sys.stdin)
if d.get('status') != 'success':
    print('Error:', d)
    sys.exit(1)
rec = d.get('updated_record') or (d.get('updated_records') or [{}])[0]
print(f'[+] Applied Reputation Event -> Peer={rec.get(\"peer_id\",\"all\")} | Score={rec.get(\"trust_score\")} | Tier={rec.get(\"trust_tier\")} | Completed={rec.get(\"blocks_completed\")} | Missed={rec.get(\"blocks_missed\")} | Forgiven={rec.get(\"straggler_forgiven\")}')
"
    ;;

  rep-reset)
    TARGET_ARG="${2:-all}"
    curl -s -X POST -H "Content-Type: application/json" \
      -d "{\"target\": \"${TARGET_ARG}\", \"event\": \"manual_reset\", \"count\": 1}" \
      "${DASHBOARD_API}/api/reputation/event" > /dev/null
    echo "[+] Reset reputation scores for target '${TARGET_ARG}' back to 60.0 (STANDARD)."
    "$0" rep-status
    ;;

  *)
    echo "Usage:"
    echo "  $0 status [block_id]"
    echo "  $0 join-store [auto|0..7] [col]"
    echo "  $0 leave-store <port>"
    echo "  $0 join-light"
    echo "  $0 leave-light <port>"
    echo "  $0 das <block_id_or_height> [samples] [light_port]"
    echo "  $0 das-cell <block_id_or_height> <row> <col> [light_port]"
    echo "  $0 rep-status"
    echo "  $0 rep-sim <port_or_peer_or_all> <block_completed|das_served|missed_block|fetch_timeout|ungraceful_drop|byzantine_fault|manual_reset> [count] [block_id]"
    echo "  $0 rep-reset [port_or_peer_or_all]"
    exit 1
    ;;
esac
