#!/usr/bin/env python3
import argparse
import json
import os

def generate_compose(k, k_piece, cols, stores_per_col, lights, crash_on_fail=False, active_cols=None, prune_enable=False, prune_ttl=None):
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
    compose['services']['publisher'] = {
        'build': {
            'context': '.',
            'dockerfile': 'Dockerfile'
        },
        'command': ["/usr/local/bin/publisher", "-config", "/app/publisher_config.json"],
        'ports': ["8080:8080", "18080:18080"],
        'volumes': ["./publisher_config_docker.json:/app/publisher_config.json"],
        'networks': ['cda-net']
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

        compose['services'][bootstrap_name] = {
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
            'ports': [f"{bootstrap_port}:{bootstrap_port}", f"{bootstrap_p2p_port}:{bootstrap_p2p_port}"],
            'networks': ['cda-net'],
            'depends_on': ['publisher']
        }

        bootstrap_addresses.append(f"{c}:/dns4/{bootstrap_name}/tcp/{bootstrap_p2p_port}")

        # Store Nodes for this column
        for s in range(1, stores_per_col + 1):
            store_name = f'store-{c}-{s}'
            store_ports.append(current_store_host_port)

            compose['services'][store_name] = {
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
                'ports': [f"{current_store_host_port}:8080", f"{current_store_host_port + 10000}:18080"],
                'volumes': [f"./data/store_{current_store_host_port}:/app/data/store_8080"],
                'networks': ['cda-net'],
                'depends_on': [bootstrap_name]
            }
            current_store_host_port += 1

    # Light Nodes
    bootstraps_arg = ";".join(bootstrap_addresses)
    for l in range(1, lights + 1):
        light_port = 9400 + l
        light_name = f'light-{l}'

        compose['services'][light_name] = {
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
                "-num-cols", str(cols)
            ] + crash_arg,
            'ports': [f"{light_port}:{light_port}", f"{light_port + 10000}:{light_port + 10000}"],
            'volumes': [f"./data/light_{light_port}:/app/data/light_{light_port}"],
            'networks': ['cda-net'],
            'depends_on': ['publisher']
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

def generate_prometheus_config(active_cols, stores_per_col, lights):
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
                "title": "Healthy / Online Nodes",
                "gridPos": {"h": 4, "w": 6, "x": 0, "y": 0},
                "targets": [
                    {"expr": "sum(up)", "legendFormat": "Nodes Up"}
                ],
                "options": {
                    "colorMode": "value",
                    "graphMode": "area",
                    "justifyMode": "center",
                    "textMode": "value"
                }
            },
            {
                "type": "gauge",
                "title": "DAS Success Rate",
                "gridPos": {"h": 6, "w": 6, "x": 6, "y": 0},
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
                "title": "Byzantine Forged Pieces Blocked",
                "gridPos": {"h": 4, "w": 6, "x": 12, "y": 0},
                "targets": [
                    {"expr": "sum(cda_store_byzantine_detection_count)", "legendFormat": "Detections"}
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
                "title": "Publisher Throughput",
                "gridPos": {"h": 4, "w": 6, "x": 18, "y": 0},
                "targets": [
                    {"expr": "sum(rate(cda_publisher_throughput_bytes_total[5s]))", "legendFormat": "Bytes/sec"}
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
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 6},
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
                "title": "Node Encoding & Proof Durations",
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 6},
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
                "title": "Total Stored Pieces per Store Node (Custody + Recoded Non-Custody)",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 14},
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
                "type": "graph",
                "title": "Store Node Database Size on Disk (BadgerDB)",
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 14},
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
                "type": "table",
                "title": "Store Node Total Stored Pieces Table",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 22},
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
                "title": "Store Node Network Rates (P2P Queries & GossipSub Propagation)",
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 22},
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
                "title": "System CPU Usage (%)",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 30},
                **line_graph_opts(),
                "targets": [
                    {
                        "expr": "rate(process_cpu_seconds_total[5s]) * 100",
                        "legendFormat": "{{job}} ({{instance}})"
                    }
                ]
            },
            {
                "type": "graph",
                "title": "System Memory Usage (MB)",
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 30},
                **line_graph_opts(),
                "targets": [
                    {
                        "expr": "process_resident_memory_bytes / 1024 / 1024",
                        "legendFormat": "{{job}} ({{instance}})"
                    }
                ]
            }
        ]
    }
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
    parser.add_argument('--k', type=int, default=16, help='K parameter for erasure coding (matrix size)')
    parser.add_argument('--k-piece', type=int, default=4, help='KPiece parameter for RLNC/KZG')
    parser.add_argument('--cols', type=int, default=8, help='Number of columns to simulate')
    parser.add_argument('--active-cols', type=int, default=None, help='Number of active columns to run in Compose')
    parser.add_argument('--stores-per-col', type=int, default=8, help='Number of store nodes per column')
    parser.add_argument('--lights', type=int, default=2, help='Number of light nodes')
    parser.add_argument('--crash-on-fail', action='store_true', help='Enable crash on fail for nodes')
    parser.add_argument('--prune-enable', action='store_true', help='Enable pruning for store and bootstrap nodes')
    parser.add_argument('--prune-ttl', type=str, default=None, help='TTL duration before pruning (e.g. 5m)')
    parser.add_argument('--out', type=str, default='docker-compose.json', help='Output file')
    
    args = parser.parse_args()

    active_cols = args.active_cols if args.active_cols is not None else args.cols
    compose_dict, store_ports = generate_compose(args.k, args.k_piece, args.cols, args.stores_per_col, args.lights, args.crash_on_fail, active_cols, args.prune_enable, args.prune_ttl)
    
    n = 2 * args.k
    cols_per_net_col = n // args.cols if args.cols > 0 else 1
    
    generate_prometheus_config(active_cols, args.stores_per_col, args.lights)
    generate_grafana_provisioning()
    generate_publisher_config(args.k, args.k_piece, args.cols, cols_per_net_col, active_cols)
    
    with open(args.out, 'w') as f:
        json.dump(compose_dict, f, indent=2)
        
    print(f"Generated {args.out} with {args.cols} cols, {args.stores_per_col} stores/col, {args.lights} light nodes.")
    print(f"STORE_PORTS={' '.join(map(str, store_ports))}")
