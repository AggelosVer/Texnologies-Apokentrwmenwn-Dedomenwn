import random
import statistics
import json
from typing import List, Dict, Tuple
from chord_node import ChordNode

class ChordHopAnalyzer:
    def __init__(self, m_bits: int = 160):
        self.m_bits = m_bits
        self.nodes: List[ChordNode] = []
        
    def create_nodes(self, num_nodes: int) -> List[ChordNode]:
        nodes = []
        for i in range(num_nodes):
            node = ChordNode(f"192.168.1.{i}", 5000 + i, self.m_bits)
            nodes.append(node)
        return nodes
    
    def count_hops(self, start_node: ChordNode, key: str) -> int:
        hops = 0
        key_id = start_node.hasher.hash_key(key)
        current = start_node
        
        while True:
            if start_node.hasher.in_range(key_id, current.id, current.successor.id, 
                                         inclusive_start=False, inclusive_end=True):
                hops += 1
                break
            
            next_node = current.closest_preceding_node(key_id)
            if next_node is current:
                hops += 1
                break
            
            current = next_node
            hops += 1
            
        return hops
    
    def measure_hops(self, num_nodes: int, num_keys: int, num_lookups: int) -> Dict:
        nodes = self.create_nodes(num_nodes)
        nodes[0].create_ring()
        
        for i in range(1, num_nodes):
            nodes[i].join(nodes[0], init_fingers=True, transfer_data=False)
        
        for node in nodes:
            node.fix_fingers()
        
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
            result = self.measure_hops(num_nodes, size, num_lookups)
            results.append(result)
            
        return results

if __name__ == "__main__":
    analyzer = ChordHopAnalyzer(m_bits=160)
    
    sizes = [1000, 5000, 10000, 25000, 50000, 75000, 100000]
    
    results = analyzer.run_analysis(sizes, num_nodes=20, num_lookups=100)
    
    with open('chord_hop_results.json', 'w') as f:
        json.dump(results, f, indent=2)
