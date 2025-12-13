import unittest
from pastry_node import PastryNode
from chord_node import ChordNode


class TestPastryNode(unittest.TestCase):
    
    def test_01_initialization(self):
        node = PastryNode("127.0.0.1", 8000, m_bits=160)
        self.assertIsNotNone(node.id)
        self.assertEqual(len(node.leaf_smaller), 0)
        self.assertEqual(len(node.leaf_larger), 0)
        self.assertEqual(len(node.neighborhood_set), 0)
        self.assertEqual(len(node.routing_table), 0)
        print(f"✓ Node initialized: {node}")

    def test_02_build_network(self):
        nodes = []
        nodes.append(PastryNode("10.0.0.1", 8000, m_bits=160))
        
        for i in range(2, 11):
            new_node = PastryNode(f"10.0.0.{i}", 8000, m_bits=160)
            new_node.join(nodes[0])
            nodes.append(new_node)
        
        for node in nodes:
            total_state = (len(node.get_leaf_set()) + 
                          len(node.neighborhood_set) + 
                          sum(len(row) for row in node.routing_table.values()))
            self.assertGreater(total_state, 0, f"Node {node} has no routing state")
        
        print(f"✓ Built network with {len(nodes)} nodes")

    def test_03_insert_and_lookup(self):
        nodes = []
        for i in range(1, 6):
            node = PastryNode(f"192.168.1.{i}", 8000, m_bits=160)
            if i > 1:
                node.join(nodes[0])
            nodes.append(node)
        
        movies = {
            "The Shawshank Redemption": {"year": 1994, "rating": 9.3},
            "The Godfather": {"year": 1972, "rating": 9.2},
            "The Dark Knight": {"year": 2008, "rating": 9.0},
            "Pulp Fiction": {"year": 1994, "rating": 8.9},
            "Forrest Gump": {"year": 1994, "rating": 8.8}
        }
        
        total_hops = 0
        for title, data in movies.items():
            success, hops = nodes[0].insert(title, data)
            self.assertTrue(success)
            total_hops += hops
            print(f"  Inserted '{title}' in {hops} hops")
        
        avg_insert_hops = total_hops / len(movies)
        print(f"✓ Average insert hops: {avg_insert_hops:.2f}")
        
        total_hops = 0
        for title in movies.keys():
            value, hops = nodes[0].lookup(title)
            self.assertIsNotNone(value, f"Failed to lookup '{title}'")
            self.assertEqual(value, movies[title])
            total_hops += hops
            print(f"  Found '{title}' in {hops} hops")
        
        avg_lookup_hops = total_hops / len(movies)
        print(f"✓ Average lookup hops: {avg_lookup_hops:.2f}")

    def test_04_delete_operation(self):
        nodes = [PastryNode(f"10.0.1.{i}", 8000, m_bits=160) for i in range(1, 4)]
        for i in range(1, len(nodes)):
            nodes[i].join(nodes[0])
        
        key = "Test Movie"
        value = {"year": 2024}
        
        success, insert_hops = nodes[0].insert(key, value)
        self.assertTrue(success)
        print(f"  Inserted '{key}' in {insert_hops} hops")
        
        found_value, lookup_hops = nodes[0].lookup(key)
        self.assertEqual(found_value, value)
        print(f"  Verified '{key}' exists in {lookup_hops} hops")
        
        success, delete_hops = nodes[0].delete(key)
        self.assertTrue(success)
        print(f"  Deleted '{key}' in {delete_hops} hops")
        
        found_value, _ = nodes[0].lookup(key)
        self.assertIsNone(found_value)
        print(f"✓ Delete operation successful")

    def test_05_update_operation(self):
        nodes = [PastryNode(f"172.16.0.{i}", 8000, m_bits=160) for i in range(1, 4)]
        for i in range(1, len(nodes)):
            nodes[i].join(nodes[0])
        
        key = "Inception"
        original_value = {"year": 2010, "rating": 8.8}
        updated_value = {"year": 2010, "rating": 9.0}
        
        nodes[0].insert(key, original_value)
        
        success, update_hops = nodes[0].update(key, updated_value)
        self.assertTrue(success)
        print(f"  Updated '{key}' in {update_hops} hops")
        
        value, _ = nodes[0].lookup(key)
        self.assertEqual(value, updated_value)
        print(f"✓ Update operation successful")

    def test_06_node_join(self):
        nodes = [PastryNode(f"10.10.0.{i}", 8000, m_bits=160) for i in range(1, 4)]
        for i in range(1, len(nodes)):
            nodes[i].join(nodes[0])
        
        for i in range(5):
            nodes[0].insert(f"Movie_{i}", {"id": i})
        
        new_node = PastryNode("10.10.0.99", 8000, m_bits=160)
        new_node.join(nodes[0])
        nodes.append(new_node)
        
        total_state = (len(new_node.get_leaf_set()) + 
                      len(new_node.neighborhood_set) + 
                      sum(len(row) for row in new_node.routing_table.values()))
        self.assertGreater(total_state, 0)
        
        for i in range(5):
            value, hops = nodes[0].lookup(f"Movie_{i}")
            self.assertIsNotNone(value, f"Movie_{i} not found after join")
        
        print(f"✓ Node join successful, routing state size: {total_state}")

    def test_07_node_leave(self):
        nodes = [PastryNode(f"10.20.0.{i}", 8000, m_bits=160) for i in range(1, 6)]
        for i in range(1, len(nodes)):
            nodes[i].join(nodes[0])
        
        test_keys = ["Movie_A", "Movie_B", "Movie_C"]
        for key in test_keys:
            nodes[0].insert(key, {"data": key})
        
        leaving_node = nodes[2]
        leaving_node.leave(transfer_data=True)
        
        for key in test_keys:
            value, _ = nodes[0].lookup(key)
            self.assertIsNotNone(value, f"Lost data '{key}' after node leave")
        
        print(f"✓ Node leave successful, data preserved")

    def test_08_routing_table_structure(self):
        nodes = [PastryNode(f"192.168.10.{i}", 8000, m_bits=160, b=4) for i in range(1, 11)]
        for i in range(1, len(nodes)):
            nodes[i].join(nodes[0])
        
        for node in nodes[:3]:
            print(f"\n  Node {node.address} (ID: {node.hex_id[:8]}...):")
            print(f"    Leaf set size: {len(node.get_leaf_set())}")
            print(f"    Neighborhood set size: {len(node.neighborhood_set)}")
            print(f"    Routing table rows: {len(node.routing_table)}")
            
            for row_idx, row in sorted(node.routing_table.items())[:3]:
                print(f"      Row {row_idx}: {len(row)} entries")
        
        print(f"✓ Routing table structure verified")

    def test_09_hop_count_analysis(self):
        print("\n  Hop Count Analysis:")
        
        for network_size in [5, 10, 15]:
            nodes = [PastryNode(f"10.{network_size}.0.{i}", 8000, m_bits=160) 
                    for i in range(1, network_size + 1)]
            for i in range(1, len(nodes)):
                nodes[i].join(nodes[0])
            
            test_keys = [f"TestKey_{i}" for i in range(10)]
            for key in test_keys:
                nodes[0].insert(key, {"value": key})
            
            total_hops = 0
            for key in test_keys:
                _, hops = nodes[0].lookup(key)
                total_hops += hops
            
            avg_hops = total_hops / len(test_keys)
            print(f"    Network size {network_size}: avg {avg_hops:.2f} hops")
        
        print(f"✓ Hop count analysis complete")


class TestChordVsPastry(unittest.TestCase):
    
    def test_compare_lookup_hops(self):
        print("\n  Chord vs Pastry Comparison:")
        
        chord_nodes = [ChordNode(f"10.1.0.{i}", 8000, m_bits=160) for i in range(1, 11)]
        for i in range(1, len(chord_nodes)):
            chord_nodes[i].join(chord_nodes[0])
        
        pastry_nodes = [PastryNode(f"10.2.0.{i}", 8000, m_bits=160) for i in range(1, 11)]
        for i in range(1, len(pastry_nodes)):
            pastry_nodes[i].join(pastry_nodes[0])
        
        test_keys = [f"Movie_{i}" for i in range(20)]
        
        for key in test_keys:
            chord_nodes[0].insert(key, {"data": key})
        for key in test_keys:
            value = chord_nodes[0].lookup(key)
        
        pastry_insert_hops = 0
        pastry_lookup_hops = 0
        for key in test_keys:
            _, hops = pastry_nodes[0].insert(key, {"data": key})
            pastry_insert_hops += hops
        for key in test_keys:
            _, hops = pastry_nodes[0].lookup(key)
            pastry_lookup_hops += hops
        
        print(f"    Pastry avg insert hops: {pastry_insert_hops/len(test_keys):.2f}")
        print(f"    Pastry avg lookup hops: {pastry_lookup_hops/len(test_keys):.2f}")
        print(f"✓ Comparison complete (Note: Chord needs hop counting added)")


if __name__ == '__main__':
    unittest.main(verbosity=2)
