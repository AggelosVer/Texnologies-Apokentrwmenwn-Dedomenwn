import time
import random
import statistics
import csv
from chord_node import ChordNode
from pastry_node import PastryNode
from typing import List, Dict, Any

class PerformanceComparison:
    def __init__(self, m_bits: int = 160, b: int = 4, l: int = 16, m: int = 32):
        self.m_bits = m_bits
        self.b = b
        self.l = l
        self.m = m
        
    def create_chord_nodes(self, num_nodes: int) -> List[ChordNode]:
        nodes = []
        for i in range(num_nodes):
            node = ChordNode("127.0.0.1", 5000 + i, self.m_bits)
            nodes.append(node)
        return nodes
    
    def create_pastry_nodes(self, num_nodes: int) -> List[PastryNode]:
        nodes = []
        for i in range(num_nodes):
            node = PastryNode("127.0.0.1", 5000 + i, self.m_bits, self.b, self.l, self.m)
            nodes.append(node)
        return nodes
    
    def benchmark_chord_build(self, num_nodes: int, num_runs: int = 5) -> Dict[str, float]:
        times = []
        
        for run in range(num_runs):
            nodes = self.create_chord_nodes(num_nodes)
            
            start_time = time.time()
            
            nodes[0].create_ring()
            
            for i in range(1, num_nodes):
                nodes[i].join(nodes[0], init_fingers=True, transfer_data=False)
            
            for _ in range(5):
                for node in nodes:
                    node.stabilize()
                    for _ in range(10):
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
    
    def benchmark_pastry_build(self, num_nodes: int, num_runs: int = 5) -> Dict[str, float]:
        times = []
        
        for run in range(num_runs):
            nodes = self.create_pastry_nodes(num_nodes)
            
            start_time = time.time()
            
            for i in range(1, num_nodes):
                nodes[i].join(nodes[0])
            
            end_time = time.time()
            times.append(end_time - start_time)
        
        return {
            'mean': statistics.mean(times),
            'median': statistics.median(times),
            'stdev': statistics.stdev(times) if len(times) > 1 else 0,
            'min': min(times),
            'max': max(times)
        }
    
    def benchmark_chord_insert(self, num_nodes: int, num_inserts: int, num_runs: int = 5) -> Dict[str, float]:
        times = []
        
        for run in range(num_runs):
            nodes = self.create_chord_nodes(num_nodes)
            nodes[0].create_ring()
            
            for i in range(1, num_nodes):
                nodes[i].join(nodes[0], init_fingers=True, transfer_data=False)
            
            for _ in range(5):
                for node in nodes:
                    node.stabilize()
                    for _ in range(10):
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
    
    def benchmark_pastry_insert(self, num_nodes: int, num_inserts: int, num_runs: int = 5) -> Dict[str, float]:
        times = []
        
        for run in range(num_runs):
            nodes = self.create_pastry_nodes(num_nodes)
            
            for i in range(1, num_nodes):
                nodes[i].join(nodes[0])
            
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
    
    def benchmark_chord_delete(self, num_nodes: int, num_items: int, num_runs: int = 5) -> Dict[str, float]:
        times = []
        
        for run in range(num_runs):
            nodes = self.create_chord_nodes(num_nodes)
            nodes[0].create_ring()
            
            for i in range(1, num_nodes):
                nodes[i].join(nodes[0], init_fingers=True, transfer_data=False)
            
            for _ in range(5):
                for node in nodes:
                    node.stabilize()
                    for _ in range(10):
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
    
    def benchmark_pastry_delete(self, num_nodes: int, num_items: int, num_runs: int = 5) -> Dict[str, float]:
        times = []
        
        for run in range(num_runs):
            nodes = self.create_pastry_nodes(num_nodes)
            
            for i in range(1, num_nodes):
                nodes[i].join(nodes[0])
            
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
    
    def benchmark_chord_node_join(self, initial_nodes: int, num_joins: int, num_runs: int = 5) -> Dict[str, float]:
        times = []
        
        for run in range(num_runs):
            nodes = self.create_chord_nodes(initial_nodes)
            nodes[0].create_ring()
            
            for i in range(1, initial_nodes):
                nodes[i].join(nodes[0], init_fingers=True, transfer_data=False)
            
            for _ in range(5):
                for node in nodes:
                    node.stabilize()
                    for _ in range(10):
                        node.fix_fingers()
            
            new_nodes = self.create_chord_nodes(num_joins)
            
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
    
    def benchmark_pastry_node_join(self, initial_nodes: int, num_joins: int, num_runs: int = 5) -> Dict[str, float]:
        times = []
        
        for run in range(num_runs):
            nodes = self.create_pastry_nodes(initial_nodes)
            
            for i in range(1, initial_nodes):
                nodes[i].join(nodes[0])
            
            new_nodes = self.create_pastry_nodes(num_joins)
            
            start_time = time.time()
            
            for new_node in new_nodes:
                new_node.join(nodes[0])
            
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
    
    def benchmark_chord_node_leave(self, num_nodes: int, num_leaves: int, num_runs: int = 5) -> Dict[str, float]:
        times = []
        
        for run in range(num_runs):
            nodes = self.create_chord_nodes(num_nodes)
            nodes[0].create_ring()
            
            for i in range(1, num_nodes):
                nodes[i].join(nodes[0], init_fingers=True, transfer_data=False)
            
            for _ in range(5):
                for node in nodes:
                    node.stabilize()
                    for _ in range(10):
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
    
    def benchmark_pastry_node_leave(self, num_nodes: int, num_leaves: int, num_runs: int = 5) -> Dict[str, float]:
        times = []
        
        for run in range(num_runs):
            nodes = self.create_pastry_nodes(num_nodes)
            
            for i in range(1, num_nodes):
                nodes[i].join(nodes[0])
            
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
    
    def benchmark_chord_lookup(self, num_nodes: int, num_items: int, num_lookups: int, num_runs: int = 5) -> Dict[str, float]:
        times = []
        
        for run in range(num_runs):
            nodes = self.create_chord_nodes(num_nodes)
            nodes[0].create_ring()
            
            for i in range(1, num_nodes):
                nodes[i].join(nodes[0], init_fingers=True, transfer_data=False)
            
            for _ in range(5):
                for node in nodes:
                    node.stabilize()
                    for _ in range(10):
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
    
    def benchmark_pastry_lookup(self, num_nodes: int, num_items: int, num_lookups: int, num_runs: int = 5) -> Dict[str, float]:
        times = []
        
        for run in range(num_runs):
            nodes = self.create_pastry_nodes(num_nodes)
            
            for i in range(1, num_nodes):
                nodes[i].join(nodes[0])
            
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
    
    def benchmark_chord_update(self, num_nodes: int, num_items: int, num_updates: int, num_runs: int = 5) -> Dict[str, float]:
        times = []
        
        for run in range(num_runs):
            nodes = self.create_chord_nodes(num_nodes)
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
            for _ in range(num_updates):
                key = random.choice(keys)
                new_value = f"updated_{key}"
                random_node = random.choice(nodes)
                random_node.update(key, new_value)
            
            end_time = time.time()
            times.append(end_time - start_time)
            
        return {
            'mean': statistics.mean(times),
            'median': statistics.median(times),
            'stdev': statistics.stdev(times) if len(times) > 1 else 0,
            'min': min(times),
            'max': max(times),
            'avg_per_update': statistics.mean(times) / num_updates
        }

    def benchmark_pastry_update(self, num_nodes: int, num_items: int, num_updates: int, num_runs: int = 5) -> Dict[str, float]:
        times = []
        
        for run in range(num_runs):
            nodes = self.create_pastry_nodes(num_nodes)
            for i in range(1, num_nodes):
                nodes[i].join(nodes[0])
            
            keys = []
            for i in range(num_items):
                key = f"key_{i}"
                value = f"value_{i}"
                random_node = random.choice(nodes)
                random_node.insert(key, value)
                keys.append(key)
            
            start_time = time.time()
            for _ in range(num_updates):
                key = random.choice(keys)
                new_value = f"updated_{key}"
                random_node = random.choice(nodes)
                random_node.update(key, new_value)
            
            end_time = time.time()
            times.append(end_time - start_time)
            
        return {
            'mean': statistics.mean(times),
            'median': statistics.median(times),
            'stdev': statistics.stdev(times) if len(times) > 1 else 0,
            'min': min(times),
            'max': max(times),
            'avg_per_update': statistics.mean(times) / num_updates
        }
    
    def compare_all_operations(self):
        all_results = []
        
        print("=" * 100)
        print("CHORD VS PASTRY PERFORMANCE COMPARISON")
        print("=" * 100)
        
        print("\n1. BUILD COMPARISON")
        print("-" * 100)
        print(f"{'Nodes':<10}{'Protocol':<15}{'Mean (s)':<15}{'Median (s)':<15}{'StdDev (s)':<15}{'Min (s)':<15}{'Max (s)':<15}")
        print("-" * 100)
        for num_nodes in [5, 10, 20, 50]:
            chord_result = self.benchmark_chord_build(num_nodes, num_runs=3)
            pastry_result = self.benchmark_pastry_build(num_nodes, num_runs=3)
            
            print(f"{num_nodes:<10}{'Chord':<15}{chord_result['mean']:<15.4f}{chord_result['median']:<15.4f}{chord_result['stdev']:<15.4f}{chord_result['min']:<15.4f}{chord_result['max']:<15.4f}")
            print(f"{'':<10}{'Pastry':<15}{pastry_result['mean']:<15.4f}{pastry_result['median']:<15.4f}{pastry_result['stdev']:<15.4f}{pastry_result['min']:<15.4f}{pastry_result['max']:<15.4f}")
            
            if chord_result['mean'] < pastry_result['mean']:
                speedup = pastry_result['mean'] / chord_result['mean'] if chord_result['mean'] > 0 else 0
                faster = "Chord"
            else:
                speedup = chord_result['mean'] / pastry_result['mean'] if pastry_result['mean'] > 0 else 0
                faster = "Pastry"
            
            print(f"{'':<10}{'Speedup:':<15}{abs(speedup):<15.2f}x ({faster} is faster)")
            print("-" * 100)
            
            all_results.append({
                'operation': 'build',
                'parameter': num_nodes,
                'protocol': 'Chord',
                'mean': chord_result['mean'],
                'median': chord_result['median'],
                'stdev': chord_result['stdev'],
                'min': chord_result['min'],
                'max': chord_result['max']
            })
            all_results.append({
                'operation': 'build',
                'parameter': num_nodes,
                'protocol': 'Pastry',
                'mean': pastry_result['mean'],
                'median': pastry_result['median'],
                'stdev': pastry_result['stdev'],
                'min': pastry_result['min'],
                'max': pastry_result['max']
            })
        
        print("\n2. INSERT COMPARISON")
        print("-" * 100)
        print(f"{'Inserts':<10}{'Protocol':<15}{'Mean (s)':<15}{'Median (s)':<15}{'StdDev (s)':<15}{'Avg/Op (s)':<15}")
        print("-" * 100)
        for num_inserts in [100, 500, 1000]:
            chord_result = self.benchmark_chord_insert(10, num_inserts, num_runs=3)
            pastry_result = self.benchmark_pastry_insert(10, num_inserts, num_runs=3)
            
            print(f"{num_inserts:<10}{'Chord':<15}{chord_result['mean']:<15.4f}{chord_result['median']:<15.4f}{chord_result['stdev']:<15.4f}{chord_result['avg_per_insert']:<15.6f}")
            print(f"{'':<10}{'Pastry':<15}{pastry_result['mean']:<15.4f}{pastry_result['median']:<15.4f}{pastry_result['stdev']:<15.4f}{pastry_result['avg_per_insert']:<15.6f}")
            
            if chord_result['mean'] < pastry_result['mean']:
                speedup = pastry_result['mean'] / chord_result['mean'] if chord_result['mean'] > 0 else 0
                faster = "Chord"
            else:
                speedup = chord_result['mean'] / pastry_result['mean'] if pastry_result['mean'] > 0 else 0
                faster = "Pastry"
            
            print(f"{'':<10}{'Speedup:':<15}{abs(speedup):<15.2f}x ({faster} is faster)")
            print("-" * 100)
            
            all_results.append({
                'operation': 'insert',
                'parameter': num_inserts,
                'protocol': 'Chord',
                'mean': chord_result['mean'],
                'median': chord_result['median'],
                'stdev': chord_result['stdev'],
                'min': chord_result['min'],
                'max': chord_result['max'],
                'avg_per_op': chord_result['avg_per_insert']
            })
            all_results.append({
                'operation': 'insert',
                'parameter': num_inserts,
                'protocol': 'Pastry',
                'mean': pastry_result['mean'],
                'median': pastry_result['median'],
                'stdev': pastry_result['stdev'],
                'min': pastry_result['min'],
                'max': pastry_result['max'],
                'avg_per_op': pastry_result['avg_per_insert']
            })
        
        print("\n3. DELETE COMPARISON")
        print("-" * 100)
        print(f"{'Deletes':<10}{'Protocol':<15}{'Mean (s)':<15}{'Median (s)':<15}{'StdDev (s)':<15}{'Avg/Op (s)':<15}")
        print("-" * 100)
        for num_items in [100, 500, 1000]:
            chord_result = self.benchmark_chord_delete(10, num_items, num_runs=3)
            pastry_result = self.benchmark_pastry_delete(10, num_items, num_runs=3)
            
            print(f"{num_items:<10}{'Chord':<15}{chord_result['mean']:<15.4f}{chord_result['median']:<15.4f}{chord_result['stdev']:<15.4f}{chord_result['avg_per_delete']:<15.6f}")
            print(f"{'':<10}{'Pastry':<15}{pastry_result['mean']:<15.4f}{pastry_result['median']:<15.4f}{pastry_result['stdev']:<15.4f}{pastry_result['avg_per_delete']:<15.6f}")
            
            if chord_result['mean'] < pastry_result['mean']:
                speedup = pastry_result['mean'] / chord_result['mean'] if chord_result['mean'] > 0 else 0
                faster = "Chord"
            else:
                speedup = chord_result['mean'] / pastry_result['mean'] if pastry_result['mean'] > 0 else 0
                faster = "Pastry"
            
            print(f"{'':<10}{'Speedup:':<15}{abs(speedup):<15.2f}x ({faster} is faster)")
            print("-" * 100)
            
            all_results.append({
                'operation': 'delete',
                'parameter': num_items,
                'protocol': 'Chord',
                'mean': chord_result['mean'],
                'median': chord_result['median'],
                'stdev': chord_result['stdev'],
                'min': chord_result['min'],
                'max': chord_result['max'],
                'avg_per_op': chord_result['avg_per_delete']
            })
            all_results.append({
                'operation': 'delete',
                'parameter': num_items,
                'protocol': 'Pastry',
                'mean': pastry_result['mean'],
                'median': pastry_result['median'],
                'stdev': pastry_result['stdev'],
                'min': pastry_result['min'],
                'max': pastry_result['max'],
                'avg_per_op': pastry_result['avg_per_delete']
            })
        
        print("\n4. NODE JOIN COMPARISON")
        print("-" * 100)
        print(f"{'Joins':<10}{'Protocol':<15}{'Mean (s)':<15}{'Median (s)':<15}{'StdDev (s)':<15}{'Avg/Op (s)':<15}")
        print("-" * 100)
        for num_joins in [5, 10, 20]:
            chord_result = self.benchmark_chord_node_join(10, num_joins, num_runs=3)
            pastry_result = self.benchmark_pastry_node_join(10, num_joins, num_runs=3)
            
            print(f"{num_joins:<10}{'Chord':<15}{chord_result['mean']:<15.4f}{chord_result['median']:<15.4f}{chord_result['stdev']:<15.4f}{chord_result['avg_per_join']:<15.6f}")
            print(f"{'':<10}{'Pastry':<15}{pastry_result['mean']:<15.4f}{pastry_result['median']:<15.4f}{pastry_result['stdev']:<15.4f}{pastry_result['avg_per_join']:<15.6f}")
            
            if chord_result['mean'] < pastry_result['mean']:
                speedup = pastry_result['mean'] / chord_result['mean'] if chord_result['mean'] > 0 else 0
                faster = "Chord"
            else:
                speedup = chord_result['mean'] / pastry_result['mean'] if pastry_result['mean'] > 0 else 0
                faster = "Pastry"
            
            print(f"{'':<10}{'Speedup:':<15}{abs(speedup):<15.2f}x ({faster} is faster)")
            print("-" * 100)
            
            all_results.append({
                'operation': 'node_join',
                'parameter': num_joins,
                'protocol': 'Chord',
                'mean': chord_result['mean'],
                'median': chord_result['median'],
                'stdev': chord_result['stdev'],
                'min': chord_result['min'],
                'max': chord_result['max'],
                'avg_per_op': chord_result['avg_per_join']
            })
            all_results.append({
                'operation': 'node_join',
                'parameter': num_joins,
                'protocol': 'Pastry',
                'mean': pastry_result['mean'],
                'median': pastry_result['median'],
                'stdev': pastry_result['stdev'],
                'min': pastry_result['min'],
                'max': pastry_result['max'],
                'avg_per_op': pastry_result['avg_per_join']
            })
        
        print("\n5. NODE LEAVE COMPARISON")
        print("-" * 100)
        print(f"{'Leaves':<10}{'Protocol':<15}{'Mean (s)':<15}{'Median (s)':<15}{'StdDev (s)':<15}{'Avg/Op (s)':<15}")
        print("-" * 100)
        for num_leaves in [5, 10, 15]:
            chord_result = self.benchmark_chord_node_leave(20, num_leaves, num_runs=3)
            pastry_result = self.benchmark_pastry_node_leave(20, num_leaves, num_runs=3)
            
            print(f"{num_leaves:<10}{'Chord':<15}{chord_result['mean']:<15.4f}{chord_result['median']:<15.4f}{chord_result['stdev']:<15.4f}{chord_result['avg_per_leave']:<15.6f}")
            print(f"{'':<10}{'Pastry':<15}{pastry_result['mean']:<15.4f}{pastry_result['median']:<15.4f}{pastry_result['stdev']:<15.4f}{pastry_result['avg_per_leave']:<15.6f}")
            
            if chord_result['mean'] < pastry_result['mean']:
                speedup = pastry_result['mean'] / chord_result['mean'] if chord_result['mean'] > 0 else 0
                faster = "Chord"
            else:
                speedup = chord_result['mean'] / pastry_result['mean'] if pastry_result['mean'] > 0 else 0
                faster = "Pastry"
            
            print(f"{'':<10}{'Speedup:':<15}{abs(speedup):<15.2f}x ({faster} is faster)")
            print("-" * 100)
            
            all_results.append({
                'operation': 'node_leave',
                'parameter': num_leaves,
                'protocol': 'Chord',
                'mean': chord_result['mean'],
                'median': chord_result['median'],
                'stdev': chord_result['stdev'],
                'min': chord_result['min'],
                'max': chord_result['max'],
                'avg_per_op': chord_result['avg_per_leave']
            })
            all_results.append({
                'operation': 'node_leave',
                'parameter': num_leaves,
                'protocol': 'Pastry',
                'mean': pastry_result['mean'],
                'median': pastry_result['median'],
                'stdev': pastry_result['stdev'],
                'min': pastry_result['min'],
                'max': pastry_result['max'],
                'avg_per_op': pastry_result['avg_per_leave']
            })
        
        print("\n6. LOOKUP COMPARISON")
        print("-" * 100)
        print(f"{'Lookups':<10}{'Protocol':<15}{'Mean (s)':<15}{'Median (s)':<15}{'StdDev (s)':<15}{'Avg/Op (s)':<15}")
        print("-" * 100)
        for num_lookups in [100, 500, 1000]:
            chord_result = self.benchmark_chord_lookup(10, 500, num_lookups, num_runs=3)
            pastry_result = self.benchmark_pastry_lookup(10, 500, num_lookups, num_runs=3)
            
            print(f"{num_lookups:<10}{'Chord':<15}{chord_result['mean']:<15.4f}{chord_result['median']:<15.4f}{chord_result['stdev']:<15.4f}{chord_result['avg_per_lookup']:<15.6f}")
            print(f"{'':<10}{'Pastry':<15}{pastry_result['mean']:<15.4f}{pastry_result['median']:<15.4f}{pastry_result['stdev']:<15.4f}{pastry_result['avg_per_lookup']:<15.6f}")
            
            if chord_result['mean'] < pastry_result['mean']:
                speedup = pastry_result['mean'] / chord_result['mean'] if chord_result['mean'] > 0 else 0
                faster = "Chord"
            else:
                speedup = chord_result['mean'] / pastry_result['mean'] if pastry_result['mean'] > 0 else 0
                faster = "Pastry"
            
            print(f"{'':<10}{'Speedup:':<15}{abs(speedup):<15.2f}x ({faster} is faster)")
            print("-" * 100)
            
            all_results.append({
                'operation': 'lookup',
                'parameter': num_lookups,
                'protocol': 'Chord',
                'mean': chord_result['mean'],
                'median': chord_result['median'],
                'stdev': chord_result['stdev'],
                'min': chord_result['min'],
                'max': chord_result['max'],
                'avg_per_op': chord_result['avg_per_lookup']
            })
            all_results.append({
                'operation': 'lookup',
                'parameter': num_lookups,
                'protocol': 'Pastry',
                'mean': pastry_result['mean'],
                'median': pastry_result['median'],
                'stdev': pastry_result['stdev'],
                'min': pastry_result['min'],
                'max': pastry_result['max'],
                'avg_per_op': pastry_result['avg_per_lookup']
            })
        
        print("\n7. UPDATE COMPARISON")
        print("-" * 100)
        print(f"{'Updates':<10}{'Protocol':<15}{'Mean (s)':<15}{'Median (s)':<15}{'StdDev (s)':<15}{'Avg/Op (s)':<15}")
        print("-" * 100)
        for num_updates in [100, 500, 1000]:
            chord_result = self.benchmark_chord_update(10, 500, num_updates, num_runs=3)
            pastry_result = self.benchmark_pastry_update(10, 500, num_updates, num_runs=3)
            
            print(f"{num_updates:<10}{'Chord':<15}{chord_result['mean']:<15.4f}{chord_result['median']:<15.4f}{chord_result['stdev']:<15.4f}{chord_result['avg_per_update']:<15.6f}")
            print(f"{'':<10}{'Pastry':<15}{pastry_result['mean']:<15.4f}{pastry_result['median']:<15.4f}{pastry_result['stdev']:<15.4f}{pastry_result['avg_per_update']:<15.6f}")
            
            if chord_result['mean'] < pastry_result['mean']:
                speedup = pastry_result['mean'] / chord_result['mean'] if chord_result['mean'] > 0 else 0
                faster = "Chord"
            else:
                speedup = chord_result['mean'] / pastry_result['mean'] if pastry_result['mean'] > 0 else 0
                faster = "Pastry"
            
            print(f"{'':<10}{'Speedup:':<15}{abs(speedup):<15.2f}x ({faster} is faster)")
            print("-" * 100)
            
            all_results.append({
                'operation': 'update',
                'parameter': num_updates,
                'protocol': 'Chord',
                'mean': chord_result['mean'],
                'median': chord_result['median'],
                'stdev': chord_result['stdev'],
                'min': chord_result['min'],
                'max': chord_result['max'],
                'avg_per_op': chord_result['avg_per_update']
            })
            all_results.append({
                'operation': 'update',
                'parameter': num_updates,
                'protocol': 'Pastry',
                'mean': pastry_result['mean'],
                'median': pastry_result['median'],
                'stdev': pastry_result['stdev'],
                'min': pastry_result['min'],
                'max': pastry_result['max'],
                'avg_per_op': pastry_result['avg_per_update']
            })

        print("\n" + "=" * 100)
        print("COMPARISON COMPLETE")
        print("=" * 100)
        
        with open('chord_vs_pastry_comparison.csv', 'w', newline='') as csvfile:
            fieldnames = ['operation', 'parameter', 'protocol', 'mean', 'median', 'stdev', 'min', 'max', 'avg_per_op']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for result in all_results:
                if 'avg_per_op' not in result:
                    result['avg_per_op'] = ''
                writer.writerow(result)
        
        print("\nResults saved to chord_vs_pastry_comparison.csv")

if __name__ == "__main__":
    comparison = PerformanceComparison(m_bits=160, b=4, l=16, m=32)
    comparison.compare_all_operations()
