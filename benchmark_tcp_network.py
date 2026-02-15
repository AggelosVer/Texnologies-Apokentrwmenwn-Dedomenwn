import time
import statistics
import json
import os
from chord_network_tcp import ChordNetworkNode
from pastry_network_tcp import PastryNetworkNode
from message_protocol import MessageType

def run_protocol_benchmark(protocol_name, start_port, num_nodes=5, num_operations=20):
    print(f"\n{'='*70}")
    print(f"BENCHMARKING {protocol_name.upper()} (TCP/Network Context)")
    print(f"{'='*70}")
    
    nodes = []
    try:
        # 1. Start Nodes
        print(f"[*] Starting {num_nodes} nodes on ports {start_port}-{start_port+num_nodes-1}...")
        for i in range(num_nodes):
            port = start_port + i
            if protocol_name.lower() == "chord":
                node = ChordNetworkNode(ip="127.0.0.1", port=port)
            else:
                node = PastryNetworkNode(ip="127.0.0.1", port=port)
            node.start()
            nodes.append(node)
        
        time.sleep(1) 
        
        # 2. Form Network - Using addresses to force RemoteNode creation
        print("[*] Forming DHT network (using network addresses)...")
        if protocol_name.lower() == "chord":
            nodes[0].chord_node.create_ring()
            for i in range(1, num_nodes):
                # Join via remote node proxy to ensure TCP is used for join operations
                remote_introducer = nodes[i].get_remote_node(nodes[0].address)
                nodes[i].chord_node.join(remote_introducer)
        else:
            for i in range(1, num_nodes):
                remote_introducer = nodes[i].get_remote_node(nodes[0].address)
                # Pastry join logic might need the actual object for some internal things, 
                # but let's try to join via the remote proxy if it supports it.
                # If not, the internal route() calls will eventually use RemoteNodes.
                nodes[i].pastry_node.join(remote_introducer)
        
        print("[*] Waiting for network stabilization...")
        time.sleep(2)
        
        # 3. Running Operations
        print(f"[*] Executing {num_operations} operations via TCP requests...")
        op_latencies = []
        
        for i in range(num_operations):
            key = f"key_{i}"
            val = f"value_{i}"
            
            # Use ANY node to initiate the request via send_request
            # This GUARANTEES that a TCP message is recorded in the metrics
            initiator = nodes[i % num_nodes]
            
            # Benchmark INSERT
            start = time.time()
            initiator.send_request(initiator.address, MessageType.INSERT, key, val)
            op_latencies.append((time.time() - start) * 1000)
            
            # Benchmark LOOKUP
            start = time.time()
            initiator.send_request(initiator.address, MessageType.LOOKUP, key)
            op_latencies.append((time.time() - start) * 1000)
            
        # 4. Collection of Metrics
        print("[*] Collecting and aggregating network metrics...")
        summaries = [n.metrics.get_summary() for n in nodes if n.metrics]
        
        total_msgs_sent = sum(s['total_messages_sent'] for s in summaries)
        total_msgs_recv = sum(s['total_messages_received'] for s in summaries)
        
        # Average latency across all TCP operations recorded by all nodes
        node_latencies = []
        for s in summaries:
            for op_name, op_data in s.get('operations', {}).items():
                if op_data['message_count'] > 0:
                    node_latencies.append(op_data['avg_latency_ms'])
        
        avg_node_lat = statistics.mean(node_latencies) if node_latencies else 0
        total_throughput = sum(s['throughput_msgs_per_sec'] for s in summaries)
        
        res = {
            'protocol': protocol_name,
            'msgs_sent': total_msgs_sent,
            'msgs_received': total_msgs_recv,
            'avg_node_latency_ms': round(avg_node_lat, 2),
            'client_op_latency_ms': round(statistics.mean(op_latencies), 2),
            'throughput_total': round(total_throughput, 2)
        }
        
        print(f"\n[+] Results for {protocol_name}:")
        print(f"    - Network Messages: {total_msgs_sent}")
        print(f"    - Avg TCP Latency:  {avg_node_lat:.2f} ms")
        print(f"    - Avg Op Latency:   {res['client_op_latency_ms']:.2f} ms")
        print(f"    - Total Throughput: {total_throughput:.2f} ops/sec")
        
        return res

    except Exception as e:
        print(f"[!] Benchmark Error: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        for node in nodes:
            try: node.stop()
            except: pass

def main():
    results = []
    # Chord Benchmark
    c = run_protocol_benchmark("chord", start_port=15000, num_nodes=8, num_operations=100)
    if c: results.append(c)
    
    time.sleep(2)
    
    # Pastry Benchmark
    p = run_protocol_benchmark("pastry", start_port=16000, num_nodes=8, num_operations=100)
    if p: results.append(p)
    
    if results:
        with open('network_performance_report.json', 'w') as f:
            json.dump(results, f, indent=4)
        print("\n[*] Performance report saved to network_performance_report.json")

if __name__ == "__main__":
    main()
