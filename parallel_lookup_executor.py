from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time

class ParallelLookupExecutor:
    
    def __init__(self, dht_client, max_workers: int = 10):
        self.dht_client = dht_client
        self.max_workers = max_workers
        self.lock = Lock()
    
    def _lookup_single(self, title: str) -> tuple:
        try:
            start = time.time()
            result = self.dht_client.query_movie(title)
            elapsed = time.time() - start
            
            if result and 'popularity' in result:
                return (title, result['popularity'], elapsed, None)
            else:
                return (title, None, elapsed, "Not found")
        except Exception as e:
            return (title, None, 0, str(e))
    
    def lookup_popularity(self, titles: List[str], timeout: Optional[float] = None) -> Dict[str, Optional[float]]:
        results = {}
        start_batch = time.time()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_map = {executor.submit(self._lookup_single, title): title for title in titles}
            
            for future in as_completed(future_map, timeout=timeout):
                try:
                    title, popularity, lookup_time, error = future.result(timeout=5.0)
                    results[title] = popularity
                except Exception as e:
                    title = future_map[future]
                    results[title] = None
        
        batch_time = time.time() - start_batch
        return results
    
    def lookup_popularity_with_stats(self, titles: List[str], timeout: Optional[float] = None) -> Dict:
        results = {}
        stats = {
            'total': len(titles),
            'successful': 0,
            'failed': 0,
            'batch_time': 0.0,
            'avg_lookup_time': 0.0
        }
        
        lookup_times = []
        start_batch = time.time()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_map = {executor.submit(self._lookup_single, title): title for title in titles}
            
            for future in as_completed(future_map, timeout=timeout):
                try:
                    title, popularity, lookup_time, error = future.result(timeout=5.0)
                    if error is None and popularity is not None:
                        results[title] = popularity
                        stats['successful'] += 1
                        lookup_times.append(lookup_time)
                    else:
                        results[title] = None
                        stats['failed'] += 1
                except Exception as e:
                    title = future_map[future]
                    results[title] = None
                    stats['failed'] += 1
        
        stats['batch_time'] = time.time() - start_batch
        stats['avg_lookup_time'] = sum(lookup_times) / len(lookup_times) if lookup_times else 0.0
        
        return {'results': results, 'stats': stats}
