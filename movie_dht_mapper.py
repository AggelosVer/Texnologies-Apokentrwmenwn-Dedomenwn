from typing import List, Dict, Tuple
from chord_node import ChordNode
from dht_hash import DHTHasher
import json


class MovieDHTMapper:
    def __init__(self, m_bits: int = 160):
        self.m_bits = m_bits
        self.hasher = DHTHasher(m_bits)
        self.nodes: List[ChordNode] = []
        self.movie_key_mappings: List[Tuple[str, int, str]] = []
        
    def create_chord_ring(self, num_nodes: int = 5) -> List[ChordNode]:
        nodes = []
        for i in range(num_nodes):
            node = ChordNode("127.0.0.1", 8000 + i, self.m_bits)
            nodes.append(node)
        
        nodes.sort(key=lambda n: n.id)
        
        introducer = nodes[0]
        introducer.create_ring()
        
        for i in range(1, len(nodes)):
            nodes[i].join(introducer, init_fingers=True, transfer_data=True)
        
        print("Stabilizing ring and populating finger tables...")
        for _ in range(5):
            for node in nodes:
                node.stabilize()
                for _ in range(20):
                    node.fix_fingers()
        
        self.nodes = nodes
        return nodes
    
    def generate_sample_movies(self) -> List[Dict]:
        movies = [
            {
                'id': 1, 'title': 'The Shawshank Redemption', 'year': 1994,
                'genres': ['Drama'], 'popularity': 85.5, 'rating': 9.3,
                'vote_count': 2500000, 'runtime': 142, 'budget': 25000000,
                'revenue': 58300000, 'language': 'en', 'countries': ['US']
            },
            {
                'id': 2, 'title': 'The Godfather', 'year': 1972,
                'genres': ['Crime', 'Drama'], 'popularity': 92.1, 'rating': 9.2,
                'vote_count': 1800000, 'runtime': 175, 'budget': 6000000,
                'revenue': 245000000, 'language': 'en', 'countries': ['US']
            },
            {
                'id': 3, 'title': 'The Dark Knight', 'year': 2008,
                'genres': ['Action', 'Crime', 'Drama'], 'popularity': 95.3, 'rating': 9.0,
                'vote_count': 2600000, 'runtime': 152, 'budget': 185000000,
                'revenue': 1005000000, 'language': 'en', 'countries': ['US', 'UK']
            },
            {
                'id': 4, 'title': 'Pulp Fiction', 'year': 1994,
                'genres': ['Crime', 'Drama'], 'popularity': 88.7, 'rating': 8.9,
                'vote_count': 2000000, 'runtime': 154, 'budget': 8000000,
                'revenue': 213000000, 'language': 'en', 'countries': ['US']
            },
            {
                'id': 5, 'title': 'Forrest Gump', 'year': 1994,
                'genres': ['Drama', 'Romance'], 'popularity': 91.2, 'rating': 8.8,
                'vote_count': 2100000, 'runtime': 142, 'budget': 55000000,
                'revenue': 678000000, 'language': 'en', 'countries': ['US']
            },
            {
                'id': 6, 'title': 'Inception', 'year': 2010,
                'genres': ['Action', 'Sci-Fi', 'Thriller'], 'popularity': 94.8, 'rating': 8.8,
                'vote_count': 2300000, 'runtime': 148, 'budget': 160000000,
                'revenue': 829000000, 'language': 'en', 'countries': ['US', 'UK']
            },
            {
                'id': 7, 'title': 'The Matrix', 'year': 1999,
                'genres': ['Action', 'Sci-Fi'], 'popularity': 89.4, 'rating': 8.7,
                'vote_count': 1900000, 'runtime': 136, 'budget': 63000000,
                'revenue': 463000000, 'language': 'en', 'countries': ['US']
            },
            {
                'id': 8, 'title': 'Goodfellas', 'year': 1990,
                'genres': ['Crime', 'Drama'], 'popularity': 87.6, 'rating': 8.7,
                'vote_count': 1200000, 'runtime': 146, 'budget': 25000000,
                'revenue': 47000000, 'language': 'en', 'countries': ['US']
            },
            {
                'id': 9, 'title': 'Interstellar', 'year': 2014,
                'genres': ['Adventure', 'Drama', 'Sci-Fi'], 'popularity': 93.2, 'rating': 8.6,
                'vote_count': 1800000, 'runtime': 169, 'budget': 165000000,
                'revenue': 677000000, 'language': 'en', 'countries': ['US', 'UK']
            },
            {
                'id': 10, 'title': 'The Silence of the Lambs', 'year': 1991,
                'genres': ['Crime', 'Drama', 'Thriller'], 'popularity': 86.9, 'rating': 8.6,
                'vote_count': 1400000, 'runtime': 118, 'budget': 19000000,
                'revenue': 272000000, 'language': 'en', 'countries': ['US']
            },
            {
                'id': 11, 'title': 'Parasite', 'year': 2019,
                'genres': ['Comedy', 'Drama', 'Thriller'], 'popularity': 90.5, 'rating': 8.5,
                'vote_count': 850000, 'runtime': 132, 'budget': 11400000,
                'revenue': 258000000, 'language': 'ko', 'countries': ['KR']
            },
            {
                'id': 12, 'title': 'Spirited Away', 'year': 2001,
                'genres': ['Animation', 'Adventure', 'Fantasy'], 'popularity': 88.1, 'rating': 8.6,
                'vote_count': 750000, 'runtime': 125, 'budget': 19000000,
                'revenue': 347000000, 'language': 'ja', 'countries': ['JP']
            },
            {
                'id': 13, 'title': 'The Departed', 'year': 2006,
                'genres': ['Crime', 'Drama', 'Thriller'], 'popularity': 86.3, 'rating': 8.5,
                'vote_count': 1300000, 'runtime': 151, 'budget': 90000000,
                'revenue': 291000000, 'language': 'en', 'countries': ['US']
            },
            {
                'id': 14, 'title': 'Whiplash', 'year': 2014,
                'genres': ['Drama', 'Music'], 'popularity': 84.7, 'rating': 8.5,
                'vote_count': 900000, 'runtime': 106, 'budget': 3300000,
                'revenue': 49000000, 'language': 'en', 'countries': ['US']
            },
            {
                'id': 15, 'title': 'Gladiator', 'year': 2000,
                'genres': ['Action', 'Adventure', 'Drama'], 'popularity': 87.9, 'rating': 8.5,
                'vote_count': 1500000, 'runtime': 155, 'budget': 103000000,
                'revenue': 460000000, 'language': 'en', 'countries': ['US', 'UK']
            }
        ]
        
        print(f"\nGenerated {len(movies)} sample movies")
        return movies
    
    def load_and_map_movies(self, movies: List[Dict]) -> List[Dict]:
        print(f"\nMapping {len(movies)} movies to DHT keys...")
        
        self.movie_key_mappings = []
        
        for movie in movies:
            title = movie['title']
            key_hash = self.hasher.hash_key(title)
            hex_id = self.hasher.get_hex_id(key_hash)
            self.movie_key_mappings.append((title, key_hash, hex_id))
        
        return movies
    
    def insert_movies_into_dht(self, movies: List[Dict]) -> Dict[str, int]:
        if not self.nodes:
            raise RuntimeError("No Chord ring created. Call create_chord_ring() first.")
        
        print(f"\nInserting {len(movies)} movies into DHT...")
        
        insertion_stats = {
            'total': len(movies),
            'success': 0,
            'failed': 0,
            'node_distribution': {node.address: 0 for node in self.nodes}
        }
        
        for movie in movies:
            try:
                title = movie['title']
                
                metadata = {
                    'id': movie['id'],
                    'title': title,
                    'year': movie['year'],
                    'genres': movie['genres'],
                    'popularity': movie['popularity'],
                    'rating': movie['rating'],
                    'vote_count': movie['vote_count'],
                    'runtime': movie['runtime'],
                    'budget': movie['budget'],
                    'revenue': movie['revenue'],
                    'language': movie['language'],
                    'countries': movie['countries']
                }
                
                any_node = self.nodes[0]
                success = any_node.insert(title, metadata)
                
                if success:
                    insertion_stats['success'] += 1
                    
                    key_hash = self.hasher.hash_key(title)
                    responsible_node = any_node.find_successor(key_hash)
                    insertion_stats['node_distribution'][responsible_node.address] += 1
                else:
                    insertion_stats['failed'] += 1
                    
            except Exception as e:
                print(f"Error inserting movie '{movie.get('title', 'UNKNOWN')}': {e}")
                insertion_stats['failed'] += 1
        
        return insertion_stats
    
    def query_movie(self, title: str) -> Dict:
        if not self.nodes:
            raise RuntimeError("No Chord ring created.")
        
        any_node = self.nodes[0]
        result = any_node.lookup(title)
        return result
    
    def print_mapping_report(self, max_entries: int = 20):
        print("\n" + "=" * 100)
        print("MOVIE TITLE → DHT KEY MAPPING REPORT")
        print("=" * 100)
        
        print(f"\nTotal mappings: {len(self.movie_key_mappings)}")
        print(f"\nShowing all {len(self.movie_key_mappings)} entries:")
        print("-" * 100)
        
        for i, (title, key_hash, hex_id) in enumerate(self.movie_key_mappings[:max_entries]):
            print(f"\n{i+1}. Title: {title}")
            print(f"   DHT Key (decimal): {key_hash}")
            print(f"   DHT Key (hex):     {hex_id[:16]}...")
            
            if self.nodes:
                responsible_node = self.nodes[0].find_successor(key_hash)
                print(f"   Responsible Node:  {responsible_node.address} (ID: {self.hasher.get_hex_id(responsible_node.id)[:8]}...)")
    
    def print_ring_status(self):
        print("\n" + "=" * 100)
        print("CHORD RING STATUS")
        print("=" * 100)
        
        for i, node in enumerate(self.nodes):
            print(f"\nNode {i+1}: {node.address}")
            print(f"  ID (hex):        {self.hasher.get_hex_id(node.id)[:16]}...")
            print(f"  Successor:       {node.successor.address if node.successor else 'None'}")
            print(f"  Predecessor:     {node.predecessor.address if node.predecessor else 'None'}")
            print(f"  Data entries:    {len(node.data)}")
    
    def print_insertion_stats(self, stats: Dict):
        print("\n" + "=" * 100)
        print("INSERTION STATISTICS")
        print("=" * 100)
        
        print(f"\nTotal movies:          {stats['total']}")
        print(f"Successfully inserted: {stats['success']}")
        print(f"Failed:                {stats['failed']}")
        print(f"Success rate:          {stats['success']/stats['total']*100:.2f}%")
        
        print("\n" + "-" * 100)
        print("DATA DISTRIBUTION ACROSS NODES:")
        print("-" * 100)
        
        for node_address, count in stats['node_distribution'].items():
            percentage = (count / stats['success'] * 100) if stats['success'] > 0 else 0
            bar_length = int(percentage / 2)
            bar = '█' * bar_length
            print(f"{node_address:20s}: {count:3d} movies ({percentage:5.2f}%) {bar}")
    
    def print_sample_metadata(self, num_samples: int = 5):
        print("\n" + "=" * 100)
        print("SAMPLE MOVIE METADATA IN DHT")
        print("=" * 100)
        
        samples_shown = 0
        for title, _, _ in self.movie_key_mappings[:num_samples * 2]:
            metadata = self.query_movie(title)
            if metadata:
                print(f"\n{'='*50}")
                print(f"Title: {metadata['title']}")
                print(f"{'='*50}")
                print(f"  Year:       {metadata['year']}")
                print(f"  Genres:     {', '.join(metadata['genres']) if metadata['genres'] else 'N/A'}")
                print(f"  Popularity: {metadata['popularity']:.2f}")
                print(f"  Rating:     {metadata['rating']:.1f}/10")
                print(f"  Votes:      {metadata['vote_count']:,}")
                print(f"  Runtime:    {metadata['runtime']} minutes")
                print(f"  Budget:     ${metadata['budget']:,}")
                print(f"  Revenue:    ${metadata['revenue']:,}")
                print(f"  Language:   {metadata['language']}")
                print(f"  Countries:  {', '.join(metadata['countries']) if metadata['countries'] else 'N/A'}")
                
                samples_shown += 1
                if samples_shown >= num_samples:
                    break
    
    def export_mappings_to_json(self, filename: str = 'movie_dht_mappings.json'):
        mappings = []
        for title, key_hash, hex_id in self.movie_key_mappings:
            responsible_node = None
            if self.nodes:
                node = self.nodes[0].find_successor(key_hash)
                responsible_node = node.address
            
            mappings.append({
                'title': title,
                'dht_key_decimal': key_hash,
                'dht_key_hex': hex_id,
                'responsible_node': responsible_node
            })
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(mappings, f, indent=2, ensure_ascii=False)
        
        print(f"\nMappings exported to {filename}")


def main():
    print("=" * 100)
    print("MOVIE DHT MAPPER - Creating Movie Database with Chord DHT")
    print("=" * 100)
    
    mapper = MovieDHTMapper(m_bits=160)
    
    print("\n[STEP 1] Creating Chord Ring with 5 nodes...")
    nodes = mapper.create_chord_ring(num_nodes=5)
    print(f"Successfully created ring with {len(nodes)} nodes")
    
    print("\n[STEP 2] Generating sample movie data...")
    movies = mapper.generate_sample_movies()
    
    print("\n[STEP 3] Mapping movies to DHT keys...")
    movies = mapper.load_and_map_movies(movies)
    
    print("\n[STEP 4] Displaying movie → DHT key mappings...")
    mapper.print_mapping_report(max_entries=15)
    
    print("\n[STEP 5] Inserting movies into DHT...")
    stats = mapper.insert_movies_into_dht(movies)
    
    mapper.print_ring_status()
    
    mapper.print_insertion_stats(stats)
    
    print("\n[STEP 6] Verifying data retrieval...")
    mapper.print_sample_metadata(num_samples=5)
    
    print("\n[STEP 7] Testing lookup functionality...")
    test_title = movies[0]['title']
    print(f"\nQuerying for: '{test_title}'")
    result = mapper.query_movie(test_title)
    if result:
        print(f"✓ Found! Popularity: {result['popularity']:.2f}, Rating: {result['rating']}/10")
    else:
        print("✗ Not found")
    
    print("\n[STEP 8] Exporting mappings to JSON...")
    mapper.export_mappings_to_json('movie_dht_mappings.json')
    
    print("\n" + "=" * 100)
    print("COMPLETE! Movie database successfully created in Chord DHT")
    print("=" * 100)


if __name__ == '__main__':
    main()
