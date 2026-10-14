#!/usr/bin/env python3
import argparse
import json
import os

def generate_compose(k, k_piece, cols, stores_per_col, lights, crash_on_fail=False, active_cols=None, prune_enable=True, prune_ttl='15s',
                     store_gossip_batch_size=None, store_gossip_batch_workers=None, store_gossip_batch_ticker_ms=None,
                     store_dissemination_sem=None, store_fallback_pull_sem=None, store_sharded_workers=None,
                     bootstrap_proof_gen_sem=None, bootstrap_seeding_sem=None, bootstrap_batch_chunk_size=None,
                     bootstrap_encode_workers=None,
                     publisher_max_in_flight=None, gomaxprocs=None, cpus=None,
                     cometbft_validators=0):
    if active_cols is None:
        active_cols = cols
    crash_arg = ["-crash-on-fail=true"] if crash_on_fail else []
    compose = {
        'services': {},
        'networks': {
            'cda-net': {
                'driver': 'bridge'
            }
        }
    }

    # Publisher
    pub_env = [
        'PUBLISHER_QUEUE_TIMEOUT=300s',
        f'PUBLISHER_MAX_IN_FLIGHT={publisher_max_in_flight if publisher_max_in_flight is not None else 32}'
    ]
    if gomaxprocs is not None:
        pub_env.append(f'GOMAXPROCS={gomaxprocs}')

    compose['services']['publisher'] = {
        'build': {
            'context': '.',
            'dockerfile': 'Dockerfile'
        },
        'command': ["/usr/local/bin/publisher", "-config", "/app/publisher_config.json"],
        'ports': ["8080:8080", "18080:18080"],
        'volumes': ["./publisher_config_docker.json:/app/publisher_config.json"],
        'networks': ['cda-net'],
        'environment': pub_env
    }

    bootstrap_addresses = []
    store_ports = []
    current_store_host_port = 9300

    # Dynamic columns per net column
    n = 2 * k
    cols_per_net_col = n // cols if cols > 0 else 1

    # Columns
    for c in range(active_cols):
        col_id = c * cols_per_net_col
        bootstrap_port = 9200 + c
        bootstrap_p2p_port = bootstrap_port + 10000
        bootstrap_name = f'bootstrap-{c}'

        boot_env = []
        if bootstrap_proof_gen_sem is not None:
            boot_env.append(f'BOOTSTRAP_PROOF_GEN_SEM={bootstrap_proof_gen_sem}')
        if bootstrap_seeding_sem is not None:
            boot_env.append(f'BOOTSTRAP_SEEDING_SEM={bootstrap_seeding_sem}')
        if bootstrap_batch_chunk_size is not None:
            boot_env.append(f'BOOTSTRAP_BATCH_CHUNK_SIZE={bootstrap_batch_chunk_size}')
        if bootstrap_encode_workers is not None:
            boot_env.append(f'BOOTSTRAP_ENCODE_WORKERS={bootstrap_encode_workers}')
        if gomaxprocs is not None:
            boot_env.append(f'GOMAXPROCS={gomaxprocs}')

        boot_svc = {
            'build': {
                'context': '.',
                'dockerfile': 'Dockerfile'
            },
            'command': [
                "/usr/local/bin/bootstrap",
                "-port", str(bootstrap_port),
                "-col", str(col_id),
                "-publisher", "http://publisher:8080",
                "-k", str(k),
                "-k-piece", str(k_piece)
            ] + crash_arg + (["-prune-enable=true"] if prune_enable else []) + (["-prune-ttl", prune_ttl] if prune_ttl else []),
            'ports': [f"{bootstrap_port}:{bootstrap_port}", f"{bootstrap_p2p_port}:{bootstrap_p2p_port}", f"{bootstrap_p2p_port}:{bootstrap_p2p_port}/udp"],
            'networks': ['cda-net'],
            'depends_on': ['publisher']
        }
        if boot_env:
            boot_svc['environment'] = boot_env
        compose['services'][bootstrap_name] = boot_svc

        bootstrap_addresses.append(f"{c}:/dns4/{bootstrap_name}/tcp/{bootstrap_p2p_port}")

        # Store Nodes for this column
        for s in range(1, stores_per_col + 1):
            store_name = f'store-{c}-{s}'
            store_ports.append(current_store_host_port)

            store_env = []
            if store_gossip_batch_size is not None:
                store_env.append(f'STORE_GOSSIP_BATCH_SIZE={store_gossip_batch_size}')
            if store_gossip_batch_workers is not None:
                store_env.append(f'STORE_GOSSIP_BATCH_WORKERS={store_gossip_batch_workers}')
            if store_gossip_batch_ticker_ms is not None:
                store_env.append(f'STORE_GOSSIP_BATCH_TICKER_MS={store_gossip_batch_ticker_ms}')
            if store_dissemination_sem is not None:
                store_env.append(f'STORE_DISSEMINATION_SEM={store_dissemination_sem}')
            if store_fallback_pull_sem is not None:
                store_env.append(f'STORE_FALLBACK_PULL_SEM={store_fallback_pull_sem}')
            if store_sharded_workers is not None:
                store_env.append(f'STORE_SHARDED_WORKERS={store_sharded_workers}')
            if gomaxprocs is not None:
                store_env.append(f'GOMAXPROCS={gomaxprocs}')

            store_svc = {
                'build': {
                    'context': '.',
                    'dockerfile': 'Dockerfile'
                },
                'command': [
                    "/usr/local/bin/store",
                    "-port", "8080",
                    "-row", str(s - 1),
                    "-col", str(col_id),
                    "-publisher", "http://publisher:8080",
                    "-bootstrap", f"/dns4/{bootstrap_name}/tcp/{bootstrap_p2p_port}",
                    "-k", str(k),
                    "-k-piece", str(k_piece),
                    "-num-cols", str(cols),
                    "-stores-per-col", str(stores_per_col),
                    "-myaddr", f"http://{store_name}:8080"
                ] + crash_arg + (["-prune-enable=true"] if prune_enable else []) + (["-prune-ttl", prune_ttl] if prune_ttl else []),
                'ports': [f"{current_store_host_port}:8080", f"{current_store_host_port + 10000}:18080", f"{current_store_host_port + 10000}:18080/udp"],
                'volumes': [f"./data/store_{current_store_host_port}:/app/data/store_8080"],
                'networks': ['cda-net'],
                'depends_on': [bootstrap_name]
            }
            if store_env:
                store_svc['environment'] = store_env
            compose['services'][store_name] = store_svc
            current_store_host_port += 1

    # Light Nodes
    bootstraps_arg = ";".join(bootstrap_addresses)
    for l in range(1, lights + 1):
        light_port = 9400 + l
        light_name = f'light-{l}'

        light_env = []
        if gomaxprocs is not None:
            light_env.append(f'GOMAXPROCS={gomaxprocs}')

        light_svc = {
            'build': {
                'context': '.',
                'dockerfile': 'Dockerfile'
            },
            'command': [
                "/usr/local/bin/light",
                "-port", str(light_port),
                "-publisher", "http://publisher:8080",
                "-bootstraps", bootstraps_arg,
                "-k", str(k),
                "-k-piece", str(k_piece),
                "-num-cols", str(cols),
                "-auto-das=true"
            ] + crash_arg,
            'ports': [f"{light_port}:{light_port}", f"{light_port + 10000}:{light_port + 10000}", f"{light_port + 10000}:{light_port + 10000}/udp"],
            'volumes': [f"./data/light_{light_port}:/app/data/light_{light_port}"],
            'networks': ['cda-net'],
            'depends_on': ['publisher']
        }
        if light_env:
            light_svc['environment'] = light_env
        compose['services'][light_name] = light_svc

    # CometBFT Consensus Cluster
    if cometbft_validators and cometbft_validators > 0:
        for v in range(cometbft_validators):
            cbft_name = f'cometbft-{v}'
            cbft_env = [
                f'NODE_ID={v}',
                'PUBLISHER_URL=http://publisher:8080/publish',
                f'CDA_K={k}',
                'CDA_MAX_K=64',
                f'CDA_K_PIECE={k_piece}',
                f'DISABLE_AUTO_PUBLISHER_PUSH={"false" if v == 0 else "true"}'
            ]
            if gomaxprocs is not None:
                cbft_env.append(f'GOMAXPROCS={gomaxprocs}')

            rpc_port = 26657 if v == 0 else 27657 + v
            p2p_port = 26656 if v == 0 else 28656 + v
            cbft_svc = {
                'build': {
                    'context': '.',
                    'dockerfile': 'Dockerfile.cometbft'
                },
                'command': ["node", "--proxy_app", "kvstore"],
                'ports': [
                    f"{rpc_port}:26657",
                    f"{p2p_port}:26656"
                ],
                'volumes': [
                    f"./data/cometbft/node{v}:/cometbft"
                ],
                'networks': ['cda-net'],
                'depends_on': ['publisher'],
                'environment': cbft_env
            }
            compose['services'][cbft_name] = cbft_svc

    # Apply CPU quota limits if specified
    if cpus is not None:
        for svc_name in compose['services']:
            compose['services'][svc_name]['deploy'] = {
                'resources': {
                    'limits': {
                        'cpus': str(cpus)
                    }
                }
            }

    # Prometheus
    compose['services']['prometheus'] = {
        'image': 'prom/prometheus:latest',
        'volumes': ['./data/prometheus.yml:/etc/prometheus/prometheus.yml'],
        'ports': ['9090:9090'],
        'networks': ['cda-net']
    }

    # Grafana
    compose['services']['grafana'] = {
        'image': 'grafana/grafana:latest',
        'volumes': ['./data/grafana/provisioning:/etc/grafana/provisioning'],
        'ports': ['3000:3000'],
        'networks': ['cda-net'],
        'environment': [
            'GF_SECURITY_ADMIN_PASSWORD=admin',
            'GF_USERS_ALLOW_SIGN_UP=false'
        ],
        'depends_on': ['prometheus']
    }

    return compose, store_ports

def generate_prometheus_config(active_cols, stores_per_col, lights, cometbft_validators=0):
    os.makedirs('data', exist_ok=True)
    prometheus_yml = """global:
  scrape_interval: 1s
  evaluation_interval: 1s

scrape_configs:
  - job_name: 'publisher'
    static_configs:
      - targets: ['publisher:8080']

  - job_name: 'bootstraps'
    static_configs:
      - targets:
"""
    for c in range(active_cols):
        prometheus_yml += f"          - 'bootstrap-{c}:{9200+c}'\n"

    prometheus_yml += """
  - job_name: 'stores'
    static_configs:
      - targets:
"""
    for c in range(active_cols):
        for s in range(1, stores_per_col + 1):
            prometheus_yml += f"          - 'store-{c}-{s}:8080'\n"

    prometheus_yml += """
  - job_name: 'lights'
    static_configs:
      - targets:
"""
    for l in range(1, lights + 1):
        prometheus_yml += f"          - 'light-{l}:{9400+l}'\n"

    if cometbft_validators > 0:
        prometheus_yml += """
  - job_name: 'cometbft'
    static_configs:
      - targets:
"""
        for v in range(cometbft_validators):
            prometheus_yml += f"          - 'cometbft-{v}:26660'\n"

    with open('data/prometheus.yml', 'w') as f:
        f.write(prometheus_yml)
    print("Generated data/prometheus.yml configuration successfully.")

def generate_grafana_provisioning():
    # Create directory structure
    os.makedirs('data/grafana/provisioning/datasources', exist_ok=True)
    os.makedirs('data/grafana/provisioning/dashboards', exist_ok=True)

    # 1. Datasource config
    datasource_yml = """apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
"""
    with open('data/grafana/provisioning/datasources/datasource.yml', 'w') as f:
        f.write(datasource_yml)

    # 2. Dashboard provider config
    dashboard_yml = """apiVersion: 1
providers:
  - name: 'CDA Dashboards'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    editable: true
    options:
      path: /etc/grafana/provisioning/dashboards
"""
    with open('data/grafana/provisioning/dashboards/dashboard.yml', 'w') as f:
        f.write(dashboard_yml)

    # Common graph styling for connected line plots with visible points
    def line_graph_opts():
        return {
            "lines": True,
            "linewidth": 2,
            "points": True,
            "pointradius": 3,
            "nullPointMode": "connected",
            "steppedLine": False,
            "fill": 1,
            "tooltip": {
                "shared": True,
                "sort": 2,
                "value_type": "individual"
            }
        }

    # 3. Premium High-Resolution Dashboard JSON
    dashboard_json = {
        "id": None,
        "title": "CDA Network Performance Dashboard",
        "tags": ["cda", "production", "high-resolution"],
        "timezone": "browser",
        "schemaVersion": 26,
        "refresh": "1s",
        "time": {
            "from": "now-2m",
            "to": "now"
        },
        "timepicker": {
            "refresh_intervals": ["1s", "2s", "5s", "10s", "30s", "1m"],
            "time_options": ["1m", "2m", "5m", "15m", "1h", "6h", "12h", "24h"]
        },
        "panels": [
            {
                "type": "stat",
                "title": "CometBFT Block Height",
                "description": "Chiều cao khối hiện tại được đồng thuận bởi cụm validator CometBFT",
                "gridPos": {"h": 4, "w": 4, "x": 0, "y": 0},
                "targets": [
                    {"expr": "max(cometbft_consensus_height) or vector(0)", "legendFormat": "Height"}
                ],
                "options": {
                    "colorMode": "value",
                    "graphMode": "none",
                    "justifyMode": "center",
                    "textMode": "value"
                },
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "fixed", "fixedColor": "light-blue"}
                    }
                }
            },
            {
                "type": "stat",
                "title": "CDA BlockReady Height",
                "description": "Chiều cao khối mới nhất đã hoàn tất lưu trữ trên 8 Store Nodes và phát tín hiệu BlockReady",
                "gridPos": {"h": 4, "w": 4, "x": 4, "y": 0},
                "targets": [
                    {"expr": "cda_publisher_latest_completed_height or vector(0)", "legendFormat": "BlockReady"}
                ],
                "options": {
                    "colorMode": "value",
                    "graphMode": "none",
                    "justifyMode": "center",
                    "textMode": "value"
                },
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "fixed", "fixedColor": "green"}
                    }
                }
            },
            {
                "type": "stat",
                "title": "CDA Dimension K (ODS Matrix)",
                "description": "Kích thước ma trận ODS (KxK cells) và EDS (2Kx2K cells) của khối gần nhất",
                "gridPos": {"h": 4, "w": 4, "x": 8, "y": 0},
                "targets": [
                    {"expr": "cda_publisher_latest_k or vector(0)", "legendFormat": "K"}
                ],
                "options": {
                    "colorMode": "value",
                    "graphMode": "none",
                    "justifyMode": "center",
                    "textMode": "value"
                },
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "fixed", "fixedColor": "orange"}
                    }
                }
            },
            {
                "type": "gauge",
                "title": "CDA Pipeline In-Flight",
                "description": "Số lượng khối đang được xử lý trong hàng đợi của Publisher (tối đa 32)",
                "gridPos": {"h": 4, "w": 4, "x": 12, "y": 0},
                "targets": [
                    {"expr": "cda_publisher_in_flight_blocks or vector(0)", "legendFormat": "In-Flight"}
                ],
                "fieldConfig": {
                    "defaults": {
                        "min": 0,
                        "max": 32,
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"value": None, "color": "green"},
                                {"value": 16, "color": "orange"},
                                {"value": 28, "color": "red"}
                            ]
                        }
                    }
                }
            },
            {
                "type": "stat",
                "title": "Mempool Pending Txns",
                "description": "Số lượng giao dịch đang chờ trong mempool của CometBFT",
                "gridPos": {"h": 4, "w": 4, "x": 16, "y": 0},
                "targets": [
                    {"expr": "sum(cometbft_mempool_size) or vector(0)", "legendFormat": "Pending Txns"}
                ],
                "options": {
                    "colorMode": "value",
                    "graphMode": "none",
                    "justifyMode": "center",
                    "textMode": "value"
                },
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "fixed", "fixedColor": "yellow"}
                    }
                }
            },
            {
                "type": "stat",
                "title": "Healthy / Online Nodes",
                "gridPos": {"h": 4, "w": 4, "x": 20, "y": 0},
                "targets": [
                    {"expr": "sum(up)", "legendFormat": "Nodes Up"}
                ],
                "options": {
                    "colorMode": "value",
                    "graphMode": "none",
                    "justifyMode": "center",
                    "textMode": "value"
                }
            },
            {
                "type": "graph",
                "title": "Block Height Progression (Consensus vs CDA BlockReady)",
                "description": "So sánh tiến độ khối của CometBFT Consensus và tiến độ đạt BlockReady trên CDA",
                "gridPos": {"h": 7, "w": 12, "x": 0, "y": 4},
                **line_graph_opts(),
                "targets": [
                    {
                        "expr": "max(cometbft_consensus_height)",
                        "legendFormat": "CometBFT Consensus Height"
                    },
                    {
                        "expr": "cda_publisher_latest_completed_height",
                        "legendFormat": "CDA BlockReady Height"
                    }
                ],
                "yaxes": [
                    {"format": "short", "show": True},
                    {"format": "short", "show": False}
                ]
            },
            {
                "type": "graph",
                "title": "CDA Pipeline Latency (Publish -> BlockReady Duration)",
                "description": "Thời gian hoàn thành xử lý khối từ khi tiếp nhận đến khi 8 Store nodes custody xong (BlockReady)",
                "gridPos": {"h": 7, "w": 12, "x": 12, "y": 4},
                **line_graph_opts(),
                "targets": [
                    {
                        "expr": "rate(cda_publisher_block_duration_seconds_sum[10s]) / rate(cda_publisher_block_duration_seconds_count[10s])",
                        "legendFormat": "Avg Pipeline Latency"
                    },
                    {
                        "expr": "histogram_quantile(0.95, rate(cda_publisher_block_duration_seconds_bucket[10s]))",
                        "legendFormat": "95th Percentile Latency"
                    }
                ],
                "yaxes": [
                    {"format": "s", "show": True},
                    {"format": "short", "show": False}
                ]
            },
            {
                "type": "gauge",
                "title": "DAS Success Rate",
                "gridPos": {"h": 5, "w": 8, "x": 0, "y": 11},
                "targets": [
                    {"expr": "cda_light_das_success_rate", "legendFormat": "{{instance}}"}
                ],
                "fieldConfig": {
                    "defaults": {
                        "min": 0,
                        "max": 100,
                        "unit": "percent",
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"value": None, "color": "red"},
                                {"value": 90, "color": "orange"},
                                {"value": 99.9, "color": "green"}
                            ]
                        }
                    }
                }
            },
            {
                "type": "stat",
                "title": "Byzantine Forged Pieces Blocked (Security Alerts)",
                "description": "Số mảnh dữ liệu giả mạo bị phát hiện và ngăn chặn bởi chữ ký / kiểm chứng mật mã KZG (Layer 3)",
                "gridPos": {"h": 5, "w": 8, "x": 8, "y": 11},
                "targets": [
                    {"expr": "sum(cda_store_byzantine_detection_count)", "legendFormat": "Byzantine Detections"}
                ],
                "options": {
                    "colorMode": "value",
                    "graphMode": "none",
                    "justifyMode": "center",
                    "textMode": "value"
                },
                "fieldConfig": {
                    "defaults": {
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"value": None, "color": "green"},
                                {"value": 1, "color": "red"}
                            ]
                        }
                    }
                }
            },
            {
                "type": "stat",
                "title": "Publisher Throughput (1m Avg)",
                "description": "Lưu lượng phát hành trung bình động 1 phút",
                "gridPos": {"h": 5, "w": 8, "x": 16, "y": 11},
                "targets": [
                    {"expr": "sum(rate(cda_publisher_throughput_bytes_total[1m]))", "legendFormat": "Bytes/sec (Avg)"}
                ],
                "fieldConfig": {
                    "defaults": {
                        "unit": "Bps"
                    }
                }
            },
            {
                "type": "graph",
                "title": "DAS Sampling Latency (95th vs Average)",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 16},
                **line_graph_opts(),
                "targets": [
                    {
                        "expr": "histogram_quantile(0.95, sum(rate(cda_light_das_sample_latency_seconds_bucket[5s])) by (le))",
                        "legendFormat": "95th Percentile Latency"
                    },
                    {
                        "expr": "sum(rate(cda_light_das_sample_latency_seconds_sum[5s])) / sum(rate(cda_light_das_sample_latency_seconds_count[5s]))",
                        "legendFormat": "Average Latency"
                    }
                ],
                "yaxes": [
                    {"format": "s", "show": True},
                    {"format": "short", "show": False}
                ]
            },
            {
                "type": "graph",
                "title": "Publisher Throughput Over Time (Moving Average)",
                "description": "Sơ đồ lưu lượng phát hành trung bình theo thời gian (Moving Average 30s & 1m)",
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 16},
                **line_graph_opts(),
                "targets": [
                    {
                        "expr": "sum(rate(cda_publisher_throughput_bytes_total[30s]))",
                        "legendFormat": "Throughput (30s Moving Avg)"
                    },
                    {
                        "expr": "sum(rate(cda_publisher_throughput_bytes_total[1m]))",
                        "legendFormat": "Throughput (1m Moving Avg)"
                    }
                ],
                "yaxes": [
                    {"format": "Bps", "show": True},
                    {"format": "short", "show": False}
                ]
            },
            {
                "type": "graph",
                "title": "Total Stored Pieces per Store Node (Custody + Recoded Non-Custody)",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 24},
                **line_graph_opts(),
                "targets": [
                    {
                        "expr": "cda_store_linear_independent_pieces_count",
                        "legendFormat": "{{instance}}"
                    }
                ],
                "yaxes": [
                    {"format": "short", "show": True},
                    {"format": "short", "show": False}
                ]
            },
            {
                "type": "table",
                "title": "Store Node Total Stored Pieces Table",
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 24},
                "targets": [
                    {
                        "expr": "cda_store_linear_independent_pieces_count",
                        "legendFormat": "{{instance}}",
                        "instant": True
                    }
                ],
                "transformations": [
                    {
                        "id": "reduce",
                        "options": {
                            "reducers": ["last"]
                        }
                    },
                    {
                        "id": "organize",
                        "options": {
                            "renameByName": {
                                "Field": "Store Node Instance",
                                "Last": "Total Stored Pieces"
                            }
                        }
                    }
                ],
                "options": {
                    "showHeader": True
                }
            },
            {
                "type": "graph",
                "title": "Store Node Database Size (BadgerDB Actual Data Used)",
                "description": "Dung lượng dữ liệu thực tế đã sử dụng trong BadgerDB (User Key-Value Payload)",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 32},
                **line_graph_opts(),
                "targets": [
                    {
                        "expr": "cda_store_db_size_bytes",
                        "legendFormat": "{{instance}}"
                    }
                ],
                "yaxes": [
                    {"format": "bytes", "show": True},
                    {"format": "short", "show": False}
                ]
            },
            {
                "type": "graph",
                "title": "Store Node Network Rates (P2P Queries & GossipSub Propagation)",
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 32},
                **line_graph_opts(),
                "targets": [
                    {
                        "expr": "sum(cda_store_p2p_request_rate)",
                        "legendFormat": "Total P2P Requests/sec"
                    },
                    {
                        "expr": "sum(cda_gossipsub_message_propagation_rate)",
                        "legendFormat": "Total Gossip Pieces/sec"
                    }
                ]
            },
            {
                "type": "graph",
                "title": "Node Encoding & Proof Durations",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 40},
                **line_graph_opts(),
                "targets": [
                    {
                        "expr": "sum(rate(cda_publisher_rs_encode_duration_seconds_sum[5s])) / sum(rate(cda_publisher_rs_encode_duration_seconds_count[5s]))",
                        "legendFormat": "Publisher RS Encode (Avg)"
                    },
                    {
                        "expr": "sum(rate(cda_bootstrap_kzg_proof_duration_seconds_sum[5s])) / sum(rate(cda_bootstrap_kzg_proof_duration_seconds_count[5s]))",
                        "legendFormat": "Bootstrap KZG Proof (Avg)"
                    },
                    {
                        "expr": "sum(rate(cda_store_reconstruct_duration_seconds_sum[5s])) / sum(rate(cda_store_reconstruct_duration_seconds_count[5s]))",
                        "legendFormat": "Store Reconstruction (Avg)"
                    }
                ],
                "yaxes": [
                    {"format": "s", "show": True},
                    {"format": "short", "show": False}
                ]
            },
            {
                "type": "graph",
                "title": "System CPU Usage (Cores)",
                "description": "Số lượng CPU Cores thực tế được sử dụng (1.0 = 1 full core)",
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 40},
                **line_graph_opts(),
                "targets": [
                    {
                        "expr": "rate(process_cpu_seconds_total[5s])",
                        "legendFormat": "{{job}} ({{instance}})"
                    }
                ],
                "yaxes": [
                    {"format": "short", "label": "Cores", "show": True},
                    {"format": "short", "show": False}
                ]
            }
        ]
    }
    for idx, panel in enumerate(dashboard_json["panels"], start=1):
        panel["id"] = idx
    with open('data/grafana/provisioning/dashboards/cda_dashboard.json', 'w') as f:
        json.dump(dashboard_json, f, indent=2)
    print("Generated Grafana provisioning configuration successfully.")

def generate_publisher_config(k, k_piece, cols, cols_per_net_col, active_cols=None):
    if active_cols is None:
        active_cols = cols
    peers = {}
    for c in range(active_cols):
        bootstrap_p2p_port = 9200 + c + 10000
        peers[str(c)] = f"/dns4/bootstrap-{c}/tcp/{bootstrap_p2p_port}"
    
    pub_config = {
        "api_port": 8080,
        "k": k,
        "k_piece": k_piece,
        "active_cols": active_cols,
        "num_cols": cols,
        "bootstrap_peers": peers
    }
    with open('publisher_config_docker.json', 'w') as f:
        json.dump(pub_config, f, indent=2)
    print("Generated publisher_config_docker.json successfully.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Generate docker-compose.yml for CDA network")
    parser.add_argument('-k', '--k', type=int, default=16, help='K parameter for erasure coding (matrix size)')
    parser.add_argument('-p', '--k-piece', '--piece', type=int, default=4, help='KPiece parameter for RLNC/KZG')
    parser.add_argument('-n', '--num-cols', '--cols', '--columns', dest='cols', type=int, default=8, help='Total number of network columns (N)')
    parser.add_argument('-c', '--active-cols', type=int, default=None, help='Number of active columns to run in Compose (C)')
    parser.add_argument('-s', '--stores-per-col', type=int, default=8, help='Number of store nodes per column (S)')
    parser.add_argument('-l', '--lights', '--light-nodes', type=int, default=2, help='Number of light nodes (L)')
    parser.add_argument('--crash-on-fail', action='store_true', help='Enable crash on fail for nodes')
    parser.add_argument('--prune-enable', action='store_true', default=True, help='Enable pruning for store and bootstrap nodes (default: True)')
    parser.add_argument('--no-prune', dest='prune_enable', action='store_false', help='Disable pruning for store and bootstrap nodes')
    parser.add_argument('--prune-ttl', type=str, default='15s', help='TTL duration before pruning (default: 15s)')
    parser.add_argument('--store-gossip-batch-size', type=int, default=64, help='Store GossipSub batch verification size (default: 64)')
    parser.add_argument('--store-gossip-batch-workers', type=int, default=4, help='Store GossipSub batch worker count (default: 4)')
    parser.add_argument('--store-gossip-batch-ticker-ms', type=int, default=10, help='Store GossipSub batch flush interval in ms (default: 10)')
    parser.add_argument('--store-dissemination-sem', type=int, default=32, help='Store dissemination semaphore limit (default: 32)')
    parser.add_argument('--store-fallback-pull-sem', type=int, default=None, help='Store fallback pull semaphore limit')
    parser.add_argument('--store-sharded-workers', type=int, default=16, help='Store sharded cell worker pool size (default: 16)')
    parser.add_argument('--bootstrap-proof-gen-sem', type=int, default=None, help='Bootstrap KZG opening proof generator concurrency semaphore')
    parser.add_argument('--bootstrap-encode-workers', type=int, default=None, help='Bootstrap RLNC seed encoding parallel worker count')
    parser.add_argument('--bootstrap-seeding-sem', type=int, default=64, help='Bootstrap P2P seeding stream semaphore (default: 64)')
    parser.add_argument('--bootstrap-batch-chunk-size', type=int, default=64, help='Bootstrap P2P batch chunk size (default: 64)')
    parser.add_argument('--publisher-max-in-flight', type=int, default=None, help='Publisher max in-flight blocks')
    parser.add_argument('--cometbft-validators', type=int, default=0, help='Number of CometBFT validator nodes (default: 0)')
    parser.add_argument('--gomaxprocs', type=int, default=4, help='GOMAXPROCS for Go runtime (default: 4)')
    parser.add_argument('--cpus', type=str, default=None, help='Docker container CPU limit (e.g. 4, 8, 12)')
    parser.add_argument('--out', type=str, default='docker-compose.json', help='Output file')
    
    args = parser.parse_args()

    if args.cometbft_validators > 0:
        import sys
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        from init_cometbft_cluster import init_cometbft_cluster
        try:
            init_cometbft_cluster(num_validators=args.cometbft_validators, output_dir="./data/cometbft")
        except Exception as e:
            print(f"[Warning] Could not initialize CometBFT configs automatically ({e}).")
            print("         If running containers own data/cometbft, please run ./scripts/cleanup.sh first.")

    active_cols = args.active_cols if args.active_cols is not None else args.cols
    compose_dict, store_ports = generate_compose(
        k=args.k,
        k_piece=args.k_piece,
        cols=args.cols,
        stores_per_col=args.stores_per_col,
        lights=args.lights,
        crash_on_fail=args.crash_on_fail,
        active_cols=active_cols,
        prune_enable=args.prune_enable,
        prune_ttl=args.prune_ttl,
        store_gossip_batch_size=args.store_gossip_batch_size,
        store_gossip_batch_workers=args.store_gossip_batch_workers,
        store_gossip_batch_ticker_ms=args.store_gossip_batch_ticker_ms,
        store_dissemination_sem=args.store_dissemination_sem,
        store_fallback_pull_sem=args.store_fallback_pull_sem,
        store_sharded_workers=args.store_sharded_workers,
        bootstrap_proof_gen_sem=args.bootstrap_proof_gen_sem,
        bootstrap_encode_workers=args.bootstrap_encode_workers,
        bootstrap_seeding_sem=args.bootstrap_seeding_sem,
        bootstrap_batch_chunk_size=args.bootstrap_batch_chunk_size,
        publisher_max_in_flight=args.publisher_max_in_flight,
        gomaxprocs=args.gomaxprocs,
        cpus=args.cpus,
        cometbft_validators=args.cometbft_validators
    )
    
    n = 2 * args.k
    cols_per_net_col = n // args.cols if args.cols > 0 else 1
    
    generate_prometheus_config(active_cols, args.stores_per_col, args.lights, cometbft_validators=args.cometbft_validators)
    generate_grafana_provisioning()
    generate_publisher_config(args.k, args.k_piece, args.cols, cols_per_net_col, active_cols)
    
    with open(args.out, 'w') as f:
        json.dump(compose_dict, f, indent=2)
        
    print(f"Generated {args.out} with {args.cols} cols, {args.stores_per_col} stores/col, {args.lights} light nodes.")
    print(f"STORE_PORTS={' '.join(map(str, store_ports))}")
