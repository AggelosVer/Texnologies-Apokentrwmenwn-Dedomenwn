import time
import json
import matplotlib.pyplot as plt
from concurrent_movie_lookup import ConcurrentMovieLookup
from movie_dht_mapper import MovieDHTMapper
from movie_loader import get_movie_sample

def benchmark_concurrency():
    print("Initializing DHT and loading sample movies for concurrency benchmark...")
    

    mapper = MovieDHTMapper(m_bits=160)
    mapper.create_chord_ring(num_nodes=40)
    

    _, movies = get_movie_sample(n_samples=2000)
    mapper.insert_movies_into_dht(movies)
    
    all_titles = [m['title'] for m in movies]
    

    k_values = [1, 10, 50, 100, 200, 500]
    results = []
    
    lookup_engine = ConcurrentMovieLookup(mapper, max_workers=20)
    
    print("\nStarting Concurrency Benchmark (K-Movie Popularity)...")
    print(f"{'K':<10}{'Total Time (s)':<20}{'Avg Time/Lookup (s)':<20}")
    print("-" * 50)
    
    for k in k_values:
        test_titles = all_titles[:k]
        
        start_time = time.time()
        batch_results = lookup_engine.lookup_movies_concurrent(test_titles)
        total_time = time.time() - start_time
        
        avg_time = total_time / k
        
        print(f"{k:<10}{total_time:<20.4f}{avg_time:<20.4f}")
        
        results.append({
            'k': k,
            'total_time': total_time,
            'avg_time': avg_time,
            'success_rate': lookup_engine.stats['successful_queries'] / k
        })
    

    with open('concurrency_benchmark_results.json', 'w') as f:
        json.dump(results, f, indent=4)
    
    print("\nResults saved to concurrency_benchmark_results.json")

if __name__ == "__main__":
    benchmark_concurrency()
