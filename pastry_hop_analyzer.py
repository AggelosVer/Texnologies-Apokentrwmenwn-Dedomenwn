import random
import statistics
import json
from typing import List, Dict, Tuple
from pastry_node import PastryNode

class PastryHopAnalyzer:
    def __init__(self, m_bits: int = 160, b: int = 4, l: int = 16, m: int = 32):
        self.m_bits = m_bits
        self.b = b
        self.l = l
        self.m = m
        self.nodes: List[PastryNode] = []
        
    def create_nodes(self, num_nodes: int) -> List[PastryNode]:
        nodes = []
        for i in range(num_nodes):
            node = PastryNode(f"192.168.1.{i}", 5000 + i, self.m_bits, self.b, self.l, self.m)
            nodes.append(node)
        return nodes
    
    def count_hops(self, start_node: PastryNode, key: str) -> int:
        value, hops = start_node.lookup(key)
        return hops
    
    def measure_hops(self, num_nodes: int, num_keys: int, num_lookups: int) -> Dict:
        nodes = self.create_nodes(num_nodes)
        
        for i in range(1, num_nodes):
            nodes[i].join(nodes[0])
        
        keys = []
        for i in range(num_keys):
            key = f"key_{i}"
            value = f"value_{i}"
            random_node = random.choice(nodes)
            random_node.insert(key, value)
            keys.append(key)
        
        hop_counts = []
        for _ in range(num_lookups):
            key = random.choice(keys)
            random_node = random.choice(nodes)
            hops = self.count_hops(random_node, key)
            hop_counts.append(hops)
        
        return {
            'num_nodes': num_nodes,
            'num_keys': num_keys,
            'num_lookups': num_lookups,
            'mean_hops': statistics.mean(hop_counts),
            'median_hops': statistics.median(hop_counts),
            'min_hops': min(hop_counts),
            'max_hops': max(hop_counts),
            'stdev_hops': statistics.stdev(hop_counts) if len(hop_counts) > 1 else 0,
            'all_hops': hop_counts
        }
    
    def run_analysis(self, dataset_sizes: List[int], num_nodes: int = 20, 
                     num_lookups: int = 100) -> List[Dict]:
        results = []
        
        for size in dataset_sizes:
            print(f"Analyzing Pastry with {num_nodes} nodes and {size} keys...")
            result = self.measure_hops(num_nodes, size, num_lookups)
            print(f"  Mean hops: {result['mean_hops']:.2f}, Median: {result['median_hops']:.2f}, "
                  f"Min: {result['min_hops']}, Max: {result['max_hops']}")
            results.append(result)
            
        return results

if __name__ == "__main__":
    analyzer = PastryHopAnalyzer(m_bits=160, b=4, l=16, m=32)
    
    sizes = [1000, 5000, 10000, 25000, 50000, 75000, 100000]
    
    results = analyzer.run_analysis(sizes, num_nodes=20, num_lookups=100)
    
    with open('pastry_hop_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print("\nResults saved to pastry_hop_results.json")
