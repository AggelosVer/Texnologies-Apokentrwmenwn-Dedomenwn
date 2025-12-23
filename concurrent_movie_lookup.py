from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time
from movie_dht_mapper import MovieDHTMapper


class ConcurrentMovieLookup:
    
    def __init__(self, mapper: MovieDHTMapper, max_workers: int = 10):
        self.mapper = mapper
        self.max_workers = max_workers
        self.lock = Lock()
        self.stats = {
            'total_queries': 0,
            'successful_queries': 0,
            'failed_queries': 0,
            'total_time': 0.0
        }
    
    def lookup_single_movie(self, title: str) -> Tuple[str, Optional[Dict], Optional[str]]:
        try:
            start_time = time.time()
            result = self.mapper.query_movie(title)
            elapsed = time.time() - start_time
            
            if result:
                with self.lock:
                    self.stats['successful_queries'] += 1
                    self.stats['total_time'] += elapsed
                return (title, result, None)
            else:
                with self.lock:
                    self.stats['failed_queries'] += 1
                return (title, None, "Movie not found in DHT")
                
        except Exception as e:
            with self.lock:
                self.stats['failed_queries'] += 1
            return (title, None, f"Error: {str(e)}")
    
    def lookup_movies_concurrent(self, titles: List[str], 
                                 timeout: Optional[float] = None) -> Dict[str, Dict]:
        results = {}
        
        with self.lock:
            self.stats['total_queries'] = len(titles)
            self.stats['successful_queries'] = 0
            self.stats['failed_queries'] = 0
            self.stats['total_time'] = 0.0
        
        batch_start = time.time()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_title = {
                executor.submit(self.lookup_single_movie, title): title 
                for title in titles
            }
            
            for future in as_completed(future_to_title, timeout=timeout):
                title = future_to_title[future]
                try:
                    lookup_start = time.time()
                    movie_title, metadata, error = future.result(timeout=5.0)
                    lookup_time = time.time() - lookup_start
                    
                    if error is None and metadata:
                        results[movie_title] = {
                            'popularity': metadata.get('popularity'),
                            'metadata': metadata,
                            'status': 'success',
                            'error': None,
                            'lookup_time': lookup_time
                        }
                    else:
                        results[movie_title] = {
                            'popularity': None,
                            'metadata': None,
                            'status': 'failed',
                            'error': error,
                            'lookup_time': lookup_time
                        }
                        
                except Exception as e:
                    results[title] = {
                        'popularity': None,
                        'metadata': None,
                        'status': 'failed',
                        'error': f"Future exception: {str(e)}",
                        'lookup_time': 0.0
                    }
        
        batch_end = time.time()
        self.stats['batch_time'] = batch_end - batch_start
        
        return results
    
    def get_popularity_only(self, titles: List[str]) -> Dict[str, Optional[float]]:
        results = self.lookup_movies_concurrent(titles)
        return {title: data['popularity'] for title, data in results.items()}
