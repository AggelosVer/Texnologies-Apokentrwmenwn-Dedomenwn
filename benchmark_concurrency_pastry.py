import time
import json
import random
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from pastry_node import PastryNode
from dht_hash import DHTHasher

class MoviePastryMapper:
    def __init__(self, m_bits: int = 160, b: int = 4):
        self.m_bits = m_bits
        self.b = b
        self.hasher = DHTHasher(m_bits)
        self.nodes: List[PastryNode] = []
        
    def create_pastry_network(self, num_nodes: int = 5):
        nodes = []
        for i in range(num_nodes):
            node = PastryNode(ip="127.0.0.1", port=9000 + i, b=self.b)
            nodes.append(node)
        
        nodes.sort(key=lambda n: n.id)
        for i in range(1, len(nodes)):
            nodes[i].join(nodes[0])
            time.sleep(0.05)
        
        self.nodes = nodes
        return nodes

    def insert_movie(self, title: str, metadata: dict):
        if not self.nodes: return False
        success, _ = self.nodes[0].insert(title, metadata)
        return success

    def query_movie(self, title: str):
        if not self.nodes: return None
        val, _ = self.nodes[0].lookup(title)
        return val

class ConcurrentPastryLookup:
    def __init__(self, mapper: MoviePastryMapper, max_workers: int = 10):
        self.mapper = mapper
        self.max_workers = max_workers
        self.lock = Lock()
        self.stats = {'successful': 0, 'total': 0}
        
    def lookup_batch(self, titles: List[str]):
        results = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.mapper.query_movie, t): t for t in titles}
            for future in as_completed(futures):
                title = futures[future]
                res = future.result()
                if res:
                    with self.lock: self.stats['successful'] += 1
                results[title] = res
        return results

def run_benchmark():
    print("Initializing Pastry Network (20 nodes) for concurrency benchmark...")
    mapper = MoviePastryMapper()
    mapper.create_pastry_network(20)
    
    # Generate some dummy movie data
    movies = []
    for i in range(200):
        movies.append({'title': f"Movie_{i}", 'popularity': random.uniform(0, 100)})
        mapper.insert_movie(movies[-1]['title'], movies[-1])
    
    all_titles = [m['title'] for m in movies]
    k_values = [1, 5, 10, 20, 50, 100]
    results = []
    
    lookup_engine = ConcurrentPastryLookup(mapper, max_workers=20)
    
    print("\nStarting Pastry Concurrency Benchmark...")
    print(f"{'K':<10}{'Total Time (s)':<20}{'Avg Time/Lookup (s)':<20}")
    print("-" * 50)
    
    for k in k_values:
        test_titles = all_titles[:k]
        start_time = time.time()
        batch_results = lookup_engine.lookup_batch(test_titles)
        total_time = time.time() - start_time
        avg_time = total_time / k
        
        print(f"{k:<10}{total_time:<20.4f}{avg_time:<20.4f}")
        results.append({'k': k, 'total_time': total_time, 'avg_time': avg_time})
        
    # Plotting
    import matplotlib.pyplot as plt
    plt.figure(figsize=(10, 6))
    plt.plot([r['k'] for r in results], [r['avg_time'] for r in results], 'ro-', linewidth=2, markersize=8)
    plt.title("Concurrent Pastry Movie Lookup: Avg Time per Lookup vs. K", fontsize=14, fontweight='bold')
    plt.xlabel("Number of Movies (K)", fontsize=12)
    plt.ylabel("Average Latency (s)", fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.savefig('instances/pastry_concurrency_avg.png')
    
    plt.figure(figsize=(10, 6))
    plt.plot([r['k'] for r in results], [r['total_time'] for r in results], 'yo-', linewidth=2, markersize=8)
    plt.title("Concurrent Pastry Movie Lookup: Total Time vs. K", fontsize=14, fontweight='bold')
    plt.xlabel("Number of Movies (K)", fontsize=12)
    plt.ylabel("Total Execution Time (s)", fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.savefig('instances/pastry_concurrency_total.png')
    
    print("\nPastry Concurrency plots saved to 'instances/'")

if __name__ == "__main__":
    run_benchmark()
