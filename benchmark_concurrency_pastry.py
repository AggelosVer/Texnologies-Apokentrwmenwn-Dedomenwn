import time
import json
import random
import matplotlib.pyplot as plt
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
    print("Initializing Pastry Network (40 nodes) for concurrency benchmark...")
    mapper = MoviePastryMapper()
    mapper.create_pastry_network(40)
    
    # Generate movie data
    _, movies = get_movie_sample(n_samples=2000)
    print(f"Loaded {len(movies)} real movies for benchmark.")
    
    for movie in movies:
        mapper.insert_movie(movie['title'], movie)
    print(f"Inserted {len(movies)} movies into Pastry.")
    
    all_titles = [m['title'] for m in movies]
    k_values = [1, 10, 50, 100, 200, 500]
    results = []
    
    lookup_engine = ConcurrentPastryLookup(mapper, max_workers=20)
    
    print("\nStarting Pastry Concurrency Benchmark (K-Movie Popularity)...")
    print(f"{'K':<10}{'Total Time (s)':<20}{'Avg Time/Lookup (s)':<20}")
    print("-" * 50)
    
    for k in k_values:
        test_titles = all_titles[:k]
        start_time = time.time()
        batch_results = lookup_engine.lookup_batch(test_titles)
        total_time = time.time() - start_time
        avg_time = total_time / k
        
        print(f"{k:<10}{total_time:<20.4f}{avg_time:<20.4f}", flush=True)
        results.append({
            'k': k, 
            'total_time': total_time, 
            'avg_time': avg_time,
            'success_rate': lookup_engine.stats['successful'] / k
        })
        # Reset stats for next batch
        lookup_engine.stats['successful'] = 0
        
    # Plotting styles - Matching Chord benchmark aesthetics
    plt.style.use('bmh')
    
    # Plot 1: Average Latency vs K
    plt.figure(figsize=(10, 6))
    plt.plot([r['k'] for r in results], [r['avg_time'] for r in results], 
             'ro-', linewidth=2.5, markersize=8, label='Pastry Latency')
    plt.title("Concurrent Pastry Movie Lookup: Avg Latency", fontsize=14, fontweight='bold')
    plt.xlabel("Number of Concurrent Lookups (K)", fontsize=12)
    plt.ylabel("Average Time per Lookup (s)", fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    plt.savefig('instances/pastry_concurrency_avg.png', dpi=300)
    
    # Plot 2: Total Execution Time vs K
    plt.figure(figsize=(10, 6))
    plt.plot([r['k'] for r in results], [r['total_time'] for r in results], 
             'yo-', linewidth=2.5, markersize=8, label='Pastry Total Time')
    plt.title("Concurrent Pastry Movie Lookup: Total Throughput", fontsize=14, fontweight='bold')
    plt.xlabel("Number of Concurrent Lookups (K)", fontsize=12)
    plt.ylabel("Total Execution Time (s)", fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    plt.savefig('instances/pastry_concurrency_total.png', dpi=300)
    
    with open('pastry_concurrency_results.json', 'w') as f:
        json.dump(results, f, indent=4)
        
    print("\nPastry Concurrency plots saved to 'instances/'")
    print("Results saved to pastry_concurrency_results.json")
    print("Benchmark complete.", flush=True)

if __name__ == "__main__":
    from movie_loader import get_movie_sample
    run_benchmark()
