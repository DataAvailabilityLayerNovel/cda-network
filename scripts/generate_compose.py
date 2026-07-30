#!/usr/bin/env python3
import argparse
import json

def generate_compose(k, cols, stores_per_col, lights, crash_on_fail=False):
    crash_arg = ["-crash-on-fail=true"] if crash_on_fail else []
    compose = {
        'version': '3.8',
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
    current_store_host_port = 8082

    # Columns
    for c in range(cols):
        col_id = c * 2 # map to even columns for now (0, 2, 4, 6)
        bootstrap_port = 8090 + c
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
                "-k", str(k)
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
        light_port = 8094 + l
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
                "-k", str(k)
            ] + crash_arg,
            'ports': [f"{light_port}:{light_port}", f"{light_port + 10000}:{light_port + 10000}"],
            'networks': ['cda-net'],
            'depends_on': ['publisher']
        }

    return compose, store_ports

def http_addr(name, port):
    return f"http://{name}:{port}"

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Generate docker-compose.yml for CDA network")
    parser.add_argument('--k', type=int, default=4, help='K parameter for erasure coding (matrix size)')
    parser.add_argument('--cols', type=int, default=4, help='Number of columns to simulate')
    parser.add_argument('--stores-per-col', type=int, default=2, help='Number of store nodes per column')
    parser.add_argument('--lights', type=int, default=2, help='Number of light nodes')
    parser.add_argument('--crash-on-fail', action='store_true', help='Enable crash on fail for nodes')
    parser.add_argument('--out', type=str, default='docker-compose.json', help='Output file')
    
    args = parser.parse_args()

    compose_dict, store_ports = generate_compose(args.k, args.cols, args.stores_per_col, args.lights, args.crash_on_fail)
    
    with open(args.out, 'w') as f:
        json.dump(compose_dict, f, indent=2)
        
    print(f"Generated {args.out} with {args.cols} cols, {args.stores_per_col} stores/col, {args.lights} light nodes.")
    print(f"STORE_PORTS={' '.join(map(str, store_ports))}")
