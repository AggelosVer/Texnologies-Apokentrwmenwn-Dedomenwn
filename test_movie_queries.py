from chord_node import ChordNode
from pastry_node import PastryNode

def test_chord_movie_queries():
    node = ChordNode("127.0.0.1", 5000, m_bits=160)
    
    node.data["movie1"] = {"title": "Movie 1", "popularity": 50.0, "rating": 7.5, "year": 2010}
    node.data["movie2"] = {"title": "Movie 2", "popularity": 75.0, "rating": 8.2, "year": 2015}
    node.data["movie3"] = {"title": "Movie 3", "popularity": 30.0, "rating": 6.8, "year": 2020}
    
    results = node.local_query_by_popularity(40.0, 80.0)
    assert len(results) == 2
    
    results = node.local_query_by_rating(7.0, 8.0)
    assert len(results) == 1
    
    results = node.local_query_by_year(2015, 2020)
    assert len(results) == 2
    
    print("✓ Chord movie queries work")

def test_pastry_movie_queries():
    node = PastryNode("127.0.0.1", 5000, m_bits=160)
    
    node.data["movie1"] = {"title": "Movie 1", "popularity": 50.0, "rating": 7.5, "year": 2010}
    node.data["movie2"] = {"title": "Movie 2", "popularity": 75.0, "rating": 8.2, "year": 2015}
    node.data["movie3"] = {"title": "Movie 3", "popularity": 30.0, "rating": 6.8, "year": 2020}
    
    results = node.local_query_by_popularity(40.0, 80.0)
    assert len(results) == 2
    
    results = node.local_query_by_rating(7.0, 8.0)
    assert len(results) == 1
    
    results = node.local_query_by_year(2015, 2020)
    assert len(results) == 2
    
    print("✓ Pastry movie queries work")

if __name__ == "__main__":
    test_chord_movie_queries()
    test_pastry_movie_queries()
    print("\n All movie query tests passed!")
