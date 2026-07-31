#!/usr/bin/env python3
import argparse
import json
import os

def generate_compose(k, k_piece, cols, stores_per_col, lights, crash_on_fail=False):
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
    for c in range(cols):
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
                "-k-piece", str(k_piece)
            ] + crash_arg,
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
                    "-myaddr", f"http://{store_name}:8080"
                ] + crash_arg,
                'ports': [f"{current_store_host_port}:8080", f"{current_store_host_port + 10000}:18080"],
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
                "-k-piece", str(k_piece)
            ] + crash_arg,
            'ports': [f"{light_port}:{light_port}", f"{light_port + 10000}:{light_port + 10000}"],
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

def generate_prometheus_config(cols, stores_per_col, lights):
    os.makedirs('data', exist_ok=True)
    prometheus_yml = """global:
  scrape_interval: 2s

scrape_configs:
  - job_name: 'publisher'
    static_configs:
      - targets: ['publisher:8080']

  - job_name: 'bootstraps'
    static_configs:
      - targets:
"""
    for c in range(cols):
        prometheus_yml += f"          - 'bootstrap-{c}:{9200+c}'\n"

    prometheus_yml += """
  - job_name: 'stores'
    static_configs:
      - targets:
"""
    for c in range(cols):
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

    # 3. Simple Dashboard JSON
    dashboard_json = {
        "id": None,
        "title": "CDA Network Performance",
        "tags": ["cda"],
        "timezone": "browser",
        "schemaVersion": 16,
        "panels": [
            {
                "type": "graph",
                "title": "Node CPU Usage",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
                "targets": [
                    {
                        "expr": "rate(process_cpu_seconds_total[10s]) * 100",
                        "legendFormat": "{{job}} ({{instance}})"
                    }
                ]
            },
            {
                "type": "graph",
                "title": "Node Resident Memory (MB)",
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
                "targets": [
                    {
                        "expr": "process_resident_memory_bytes / 1024 / 1024",
                        "legendFormat": "{{job}} ({{instance}})"
                    }
                ]
            },
            {
                "type": "stat",
                "title": "Healthy / Online Nodes",
                "gridPos": {"h": 4, "w": 24, "x": 0, "y": 8},
                "targets": [
                    {
                        "expr": "sum(up)",
                        "legendFormat": "Nodes Up"
                    }
                ]
            }
        ]
    }
    with open('data/grafana/provisioning/dashboards/cda_dashboard.json', 'w') as f:
        json.dump(dashboard_json, f, indent=2)
    print("Generated Grafana provisioning configuration successfully.")

def generate_publisher_config(k, k_piece, cols, cols_per_net_col):
    peers = {}
    for c in range(cols):
        col_id = c * cols_per_net_col
        bootstrap_p2p_port = 9200 + c + 10000
        for data_col in range(col_id, col_id + cols_per_net_col):
            peers[str(data_col)] = f"/dns4/bootstrap-{c}/tcp/{bootstrap_p2p_port}"
    
    pub_config = {
        "api_port": 8080,
        "k": k,
        "k_piece": k_piece,
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
    parser.add_argument('--stores-per-col', type=int, default=8, help='Number of store nodes per column')
    parser.add_argument('--lights', type=int, default=2, help='Number of light nodes')
    parser.add_argument('--crash-on-fail', action='store_true', help='Enable crash on fail for nodes')
    parser.add_argument('--out', type=str, default='docker-compose.json', help='Output file')
    
    args = parser.parse_args()

    compose_dict, store_ports = generate_compose(args.k, args.k_piece, args.cols, args.stores_per_col, args.lights, args.crash_on_fail)
    
    n = 2 * args.k
    cols_per_net_col = n // args.cols if args.cols > 0 else 1
    
    generate_prometheus_config(args.cols, args.stores_per_col, args.lights)
    generate_grafana_provisioning()
    generate_publisher_config(args.k, args.k_piece, args.cols, cols_per_net_col)
    
    with open(args.out, 'w') as f:
        json.dump(compose_dict, f, indent=2)
        
    print(f"Generated {args.out} with {args.cols} cols, {args.stores_per_col} stores/col, {args.lights} light nodes.")
    print(f"STORE_PORTS={' '.join(map(str, store_ports))}")
