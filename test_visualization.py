import os
from chord_node import ChordNode
from chord_visualizer import visualize_chord_network


def test_chord_visualization():
    m_bits = 160
    num_nodes = 10
    
    nodes = []
    for i in range(num_nodes):
        node = ChordNode("127.0.0.1", 5000 + i, m_bits)
        nodes.append(node)
    
    nodes[0].create_ring()
    for i in range(1, num_nodes):
        nodes[i].join(nodes[0], init_fingers=True, transfer_data=False)
    
    print("Stabilizing network and fixing fingers...")
    for _ in range(10):
        for node in nodes:
            node.stabilize()
            for _ in range(20):
                node.fix_fingers()
    
    os.makedirs('instances', exist_ok=True)
    
    visualize_chord_network(nodes, 'instances/chord_network.png', show_fingers=False)
    visualize_chord_network(nodes, 'instances/chord_network_with_fingers.png', show_fingers=True)


if __name__ == "__main__":
    test_chord_visualization()
