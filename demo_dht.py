from chord_network_tcp import ChordNetworkNode
from pastry_network_tcp import PastryNetworkNode
import time

def demo_chord_network():

    print("=" * 70)
    print("CHORD NETWORK DEMO")
    print("=" * 70)
    

    print("\n1. Creating Chord nodes...")
    node1 = ChordNetworkNode(ip="127.0.0.1", port=7000)
    node2 = ChordNetworkNode(ip="127.0.0.1", port=7001)
    node3 = ChordNetworkNode(ip="127.0.0.1", port=7002)
    

    print("2. Starting nodes...")
    node1.start()
    node2.start()
    node3.start()
    time.sleep(1)
    

    print("3. Creating Chord ring...")
    node1.chord_node.create_ring()
    node2.chord_node.join(node1.chord_node)
    node3.chord_node.join(node1.chord_node)
    time.sleep(1)
    

    print("\n4. Inserting data...")
    node1.insert("movie_1", "The Matrix")
    node2.insert("movie_2", "Inception")
    node3.insert("movie_3", "Interstellar")
    print(f"    Inserted 3 movies")
    

    print("\n5. Looking up data from different nodes...")
    result1 = node1.lookup("movie_1")
    result2 = node2.lookup("movie_2")
    result3 = node3.lookup("movie_3")
    print(f"    movie_1: {result1}")
    print(f"    movie_2: {result2}")
    print(f"    movie_3: {result3}")
    

    print("\n6. Cross-node lookup (node1 looking up movie_3)...")
    result = node1.lookup("movie_3")
    print(f"    Found: {result}")
    

    print("\n7. Network information...")
    print(f"   Node 1 ID: {node1.chord_node.hasher.get_hex_id(node1.chord_node.id)[:16]}...")
    print(f"   Node 2 ID: {node2.chord_node.hasher.get_hex_id(node2.chord_node.id)[:16]}...")
    print(f"   Node 3 ID: {node3.chord_node.hasher.get_hex_id(node3.chord_node.id)[:16]}...")
    print(f"   Total keys in network: {len(node1.chord_node.data) + len(node2.chord_node.data) + len(node3.chord_node.data)}")
    

    print("\n8. Shutting down...")
    node1.chord_node.leave()
    node2.chord_node.leave()
    node3.chord_node.leave()
    node1.stop()
    node2.stop()
    node3.stop()
    print("    All nodes stopped")
    print("\n" + "=" * 70 + "\n")

def demo_pastry_network():

    print("=" * 70)
    print("PASTRY NETWORK DEMO")
    print("=" * 70)
    

    print("\n1. Creating Pastry nodes...")
    node1 = PastryNetworkNode(ip="127.0.0.1", port=8000)
    node2 = PastryNetworkNode(ip="127.0.0.1", port=8001)
    node3 = PastryNetworkNode(ip="127.0.0.1", port=8002)
    

    print("2. Starting nodes...")
    node1.start()
    node2.start()
    node3.start()
    time.sleep(1)
    

    print("3. Joining Pastry network...")
    node2.pastry_node.join(node1.pastry_node)
    node3.pastry_node.join(node1.pastry_node)
    time.sleep(1)
    

    print("\n4. Inserting data...")
    success1, hops1 = node1.insert("user_1", "Alice")
    success2, hops2 = node2.insert("user_2", "Bob")
    success3, hops3 = node3.insert("user_3", "Charlie")
    print(f"    Inserted user_1 ({hops1} hops)")
    print(f"    Inserted user_2 ({hops2} hops)")
    print(f"    Inserted user_3 ({hops3} hops)")
    

    print("\n5. Looking up data from different nodes...")
    result1, hops1 = node1.lookup("user_1")
    result2, hops2 = node2.lookup("user_2")
    result3, hops3 = node3.lookup("user_3")
    print(f"    user_1: {result1} ({hops1} hops)")
    print(f"    user_2: {result2} ({hops2} hops)")
    print(f"    user_3: {result3} ({hops3} hops)")
    

    print("\n6. Cross-node lookup (node1 looking up user_3)...")
    result, hops = node1.lookup("user_3")
    print(f"    Found: {result} ({hops} hops)")
    

    print("\n7. Network information...")
    print(f"   Node 1 ID: {node1.pastry_node.hex_id[:16]}...")
    print(f"   Node 2 ID: {node2.pastry_node.hex_id[:16]}...")
    print(f"   Node 3 ID: {node3.pastry_node.hex_id[:16]}...")
    print(f"   Total keys in network: {len(node1.pastry_node.data) + len(node2.pastry_node.data) + len(node3.pastry_node.data)}")
    

    print("\n8. Shutting down...")
    node1.pastry_node.leave()
    node2.pastry_node.leave()
    node3.pastry_node.leave()
    node1.stop()
    node2.stop()
    node3.stop()
    print("    All nodes stopped")
    print("\n" + "=" * 70 + "\n")

def demo_node_failure():

    print("NODE FAILURE DEMO (Chord)")
    print("=" * 70)
    

    print("\n1. Creating 4 Chord nodes...")
    nodes = []
    for i in range(4):
        node = ChordNetworkNode(ip="127.0.0.1", port=19000 + i)
        node.start()
        nodes.append(node)
    time.sleep(1)
    

    print("2. Creating ring...")
    nodes[0].chord_node.create_ring()
    for i in range(1, 4):
        nodes[i].chord_node.join(nodes[0].chord_node)
    time.sleep(1)
    

    print("\n3. Inserting data across nodes...")
    for i in range(10):
        nodes[i % 4].insert(f"key_{i}", f"value_{i}")
    print(f"    Inserted 10 key-value pairs")
    
    print("\n4. Data distribution before failure:")
    for i, node in enumerate(nodes):
        print(f"   Node {i}: {len(node.chord_node.data)} keys")
    

    print("\n5. Simulating failure of node 1...")
    nodes[1].chord_node.leave()
    nodes[1].stop()
    time.sleep(1)
    print("    Node 1 removed")
    

    print("\n6. Verifying data accessibility after failure...")
    accessible = 0
    for i in range(10):
        result = nodes[0].lookup(f"key_{i}")
        if result is not None:
            accessible += 1
    print(f"   ✓ {accessible}/10 keys still accessible")
    

    print("\n7. Data distribution after failure:")
    for i, node in enumerate(nodes):
        if i != 1:
            print(f"   Node {i}: {len(node.chord_node.data)} keys")
    

    print("\n8. Shutting down remaining nodes...")
    for i, node in enumerate(nodes):
        if i != 1:
            node.chord_node.leave()
            node.stop()
    print("    All nodes stopped")
    print("\n" + "=" * 70 + "\n")

def main():

    
    try:

        demo_chord_network()
        time.sleep(2)
        

        demo_pastry_network()
        time.sleep(2)
        

        demo_node_failure()
        
        print("\n Demos completed successfully!\n")

        
    except Exception as e:
        print(f"\n Error during demonstration: {e}\n")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
