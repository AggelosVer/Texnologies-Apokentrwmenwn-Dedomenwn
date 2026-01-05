import time
import random
import statistics
import csv
from chord_node import ChordNode
from typing import List, Dict, Any

class ChordBenchmark:
    def __init__(self, m_bits: int = 160):
        self.m_bits = m_bits
        self.nodes: List[ChordNode] = []
        self.results: Dict[str, Any] = {}
        
    def create_nodes(self, num_nodes: int) -> List[ChordNode]:
        nodes = []
        for i in range(num_nodes):
            node = ChordNode(f"192.168.1.{i}", 5000 + i, self.m_bits)
            nodes.append(node)
        return nodes
    
    def benchmark_build(self, num_nodes: int, num_runs: int = 5) -> Dict[str, float]:
        times = []
        
        for run in range(num_runs):
            nodes = self.create_nodes(num_nodes)
            
            start_time = time.time()
            
            nodes[0].create_ring()
            
            for i in range(1, num_nodes):
                nodes[i].join(nodes[0], init_fingers=True, transfer_data=False)
            
            for node in nodes:
                node.fix_fingers()
            
            end_time = time.time()
            times.append(end_time - start_time)
        
        return {
            'mean': statistics.mean(times),
            'median': statistics.median(times),
            'stdev': statistics.stdev(times) if len(times) > 1 else 0,
            'min': min(times),
            'max': max(times)
        }
    
    def benchmark_insert(self, num_nodes: int, num_inserts: int, num_runs: int = 5) -> Dict[str, float]:
        times = []
        
        for run in range(num_runs):
            nodes = self.create_nodes(num_nodes)
            nodes[0].create_ring()
            
            for i in range(1, num_nodes):
                nodes[i].join(nodes[0], init_fingers=True, transfer_data=False)
            
            for node in nodes:
                node.fix_fingers()
            
            start_time = time.time()
            
            for i in range(num_inserts):
                key = f"key_{i}"
                value = f"value_{i}"
                random_node = random.choice(nodes)
                random_node.insert(key, value)
            
            end_time = time.time()
            times.append(end_time - start_time)
        
        return {
            'mean': statistics.mean(times),
            'median': statistics.median(times),
            'stdev': statistics.stdev(times) if len(times) > 1 else 0,
            'min': min(times),
            'max': max(times),
            'avg_per_insert': statistics.mean(times) / num_inserts
        }
    
    def benchmark_delete(self, num_nodes: int, num_items: int, num_runs: int = 5) -> Dict[str, float]:
        times = []
        
        for run in range(num_runs):
            nodes = self.create_nodes(num_nodes)
            nodes[0].create_ring()
            
            for i in range(1, num_nodes):
                nodes[i].join(nodes[0], init_fingers=True, transfer_data=False)
            
            for node in nodes:
                node.fix_fingers()
            
            keys = []
            for i in range(num_items):
                key = f"key_{i}"
                value = f"value_{i}"
                random_node = random.choice(nodes)
                random_node.insert(key, value)
                keys.append(key)
            
            start_time = time.time()
            
            for key in keys:
                random_node = random.choice(nodes)
                random_node.delete(key)
            
            end_time = time.time()
            times.append(end_time - start_time)
        
        return {
            'mean': statistics.mean(times),
            'median': statistics.median(times),
            'stdev': statistics.stdev(times) if len(times) > 1 else 0,
            'min': min(times),
            'max': max(times),
            'avg_per_delete': statistics.mean(times) / num_items
        }
    
    def benchmark_node_join(self, initial_nodes: int, num_joins: int, num_runs: int = 5) -> Dict[str, float]:
        times = []
        
        for run in range(num_runs):
            nodes = self.create_nodes(initial_nodes)
            nodes[0].create_ring()
            
            for i in range(1, initial_nodes):
                nodes[i].join(nodes[0], init_fingers=True, transfer_data=False)
            
            for node in nodes:
                node.fix_fingers()
            
            new_nodes = self.create_nodes(num_joins)
            
            start_time = time.time()
            
            for new_node in new_nodes:
                new_node.join(nodes[0], init_fingers=True, transfer_data=False)
                new_node.fix_fingers()
            
            end_time = time.time()
            times.append(end_time - start_time)
        
        return {
            'mean': statistics.mean(times),
            'median': statistics.median(times),
            'stdev': statistics.stdev(times) if len(times) > 1 else 0,
            'min': min(times),
            'max': max(times),
            'avg_per_join': statistics.mean(times) / num_joins
        }
    
    def benchmark_node_leave(self, num_nodes: int, num_leaves: int, num_runs: int = 5) -> Dict[str, float]:
        times = []
        
        for run in range(num_runs):
            nodes = self.create_nodes(num_nodes)
            nodes[0].create_ring()
            
            for i in range(1, num_nodes):
                nodes[i].join(nodes[0], init_fingers=True, transfer_data=False)
            
            for node in nodes:
                node.fix_fingers()
            
            leaving_nodes = random.sample(nodes[1:], min(num_leaves, len(nodes) - 1))
            
            start_time = time.time()
            
            for node in leaving_nodes:
                node.leave(transfer_data=False)
            
            end_time = time.time()
            times.append(end_time - start_time)
        
        return {
            'mean': statistics.mean(times),
            'median': statistics.median(times),
            'stdev': statistics.stdev(times) if len(times) > 1 else 0,
            'min': min(times),
            'max': max(times),
            'avg_per_leave': statistics.mean(times) / num_leaves
        }
    
    def benchmark_lookup(self, num_nodes: int, num_items: int, num_lookups: int, num_runs: int = 5) -> Dict[str, float]:
        times = []
        
        for run in range(num_runs):
            nodes = self.create_nodes(num_nodes)
            nodes[0].create_ring()
            
            for i in range(1, num_nodes):
                nodes[i].join(nodes[0], init_fingers=True, transfer_data=False)
            
            for node in nodes:
                node.fix_fingers()
            
            keys = []
            for i in range(num_items):
                key = f"key_{i}"
                value = f"value_{i}"
                random_node = random.choice(nodes)
                random_node.insert(key, value)
                keys.append(key)
            
            start_time = time.time()
            
            for _ in range(num_lookups):
                key = random.choice(keys)
                random_node = random.choice(nodes)
                random_node.lookup(key)
            
            end_time = time.time()
            times.append(end_time - start_time)
        
        return {
            'mean': statistics.mean(times),
            'median': statistics.median(times),
            'stdev': statistics.stdev(times) if len(times) > 1 else 0,
            'min': min(times),
            'max': max(times),
            'avg_per_lookup': statistics.mean(times) / num_lookups
        }
    
    def run_all_benchmarks(self):
        all_results = []
        
        print("=" * 80)
        print("CHORD DHT BENCHMARK SUITE")
        print("=" * 80)
        
        print("\n1. BUILD BENCHMARK")
        print("-" * 80)
        for num_nodes in [5, 10, 20, 50]:
            result = self.benchmark_build(num_nodes, num_runs=3)
            print(f"Nodes: {num_nodes:3d} | Mean: {result['mean']:.4f}s | Median: {result['median']:.4f}s | "
                  f"StdDev: {result['stdev']:.4f}s | Min: {result['min']:.4f}s | Max: {result['max']:.4f}s")
            all_results.append({
                'operation': 'build',
                'parameter': num_nodes,
                'mean': result['mean'],
                'median': result['median'],
                'stdev': result['stdev'],
                'min': result['min'],
                'max': result['max']
            })
        
        print("\n2. INSERT BENCHMARK")
        print("-" * 80)
        for num_inserts in [100, 500, 1000]:
            result = self.benchmark_insert(10, num_inserts, num_runs=3)
            print(f"Inserts: {num_inserts:4d} | Mean: {result['mean']:.4f}s | Median: {result['median']:.4f}s | "
                  f"StdDev: {result['stdev']:.4f}s | Avg/Insert: {result['avg_per_insert']:.6f}s")
            all_results.append({
                'operation': 'insert',
                'parameter': num_inserts,
                'mean': result['mean'],
                'median': result['median'],
                'stdev': result['stdev'],
                'min': result['min'],
                'max': result['max'],
                'avg_per_op': result['avg_per_insert']
            })
        
        print("\n3. DELETE BENCHMARK")
        print("-" * 80)
        for num_items in [100, 500, 1000]:
            result = self.benchmark_delete(10, num_items, num_runs=3)
            print(f"Deletes: {num_items:4d} | Mean: {result['mean']:.4f}s | Median: {result['median']:.4f}s | "
                  f"StdDev: {result['stdev']:.4f}s | Avg/Delete: {result['avg_per_delete']:.6f}s")
            all_results.append({
                'operation': 'delete',
                'parameter': num_items,
                'mean': result['mean'],
                'median': result['median'],
                'stdev': result['stdev'],
                'min': result['min'],
                'max': result['max'],
                'avg_per_op': result['avg_per_delete']
            })
        
        print("\n4. NODE JOIN BENCHMARK")
        print("-" * 80)
        for num_joins in [5, 10, 20]:
            result = self.benchmark_node_join(10, num_joins, num_runs=3)
            print(f"Joins: {num_joins:2d} | Mean: {result['mean']:.4f}s | Median: {result['median']:.4f}s | "
                  f"StdDev: {result['stdev']:.4f}s | Avg/Join: {result['avg_per_join']:.6f}s")
            all_results.append({
                'operation': 'node_join',
                'parameter': num_joins,
                'mean': result['mean'],
                'median': result['median'],
                'stdev': result['stdev'],
                'min': result['min'],
                'max': result['max'],
                'avg_per_op': result['avg_per_join']
            })
        
        print("\n5. NODE LEAVE BENCHMARK")
        print("-" * 80)
        for num_leaves in [5, 10, 15]:
            result = self.benchmark_node_leave(20, num_leaves, num_runs=3)
            print(f"Leaves: {num_leaves:2d} | Mean: {result['mean']:.4f}s | Median: {result['median']:.4f}s | "
                  f"StdDev: {result['stdev']:.4f}s | Avg/Leave: {result['avg_per_leave']:.6f}s")
            all_results.append({
                'operation': 'node_leave',
                'parameter': num_leaves,
                'mean': result['mean'],
                'median': result['median'],
                'stdev': result['stdev'],
                'min': result['min'],
                'max': result['max'],
                'avg_per_op': result['avg_per_leave']
            })
        
        print("\n6. LOOKUP BENCHMARK")
        print("-" * 80)
        for num_lookups in [100, 500, 1000]:
            result = self.benchmark_lookup(10, 500, num_lookups, num_runs=3)
            print(f"Lookups: {num_lookups:4d} | Mean: {result['mean']:.4f}s | Median: {result['median']:.4f}s | "
                  f"StdDev: {result['stdev']:.4f}s | Avg/Lookup: {result['avg_per_lookup']:.6f}s")
            all_results.append({
                'operation': 'lookup',
                'parameter': num_lookups,
                'mean': result['mean'],
                'median': result['median'],
                'stdev': result['stdev'],
                'min': result['min'],
                'max': result['max'],
                'avg_per_op': result['avg_per_lookup']
            })
        
        print("\n" + "=" * 80)
        print("BENCHMARK COMPLETE")
        print("=" * 80)
        
        with open('chord_benchmark_results.csv', 'w', newline='') as csvfile:
            fieldnames = ['operation', 'parameter', 'mean', 'median', 'stdev', 'min', 'max', 'avg_per_op']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for result in all_results:
                if 'avg_per_op' not in result:
                    result['avg_per_op'] = ''
                writer.writerow(result)
        
        print("\nResults saved to chord_benchmark_results.csv")

if __name__ == "__main__":
    benchmark = ChordBenchmark(m_bits=160)
    benchmark.run_all_benchmarks()
