from chord_node import ChordNode
from chord_visualizer import visualize_chord_network


def test_chord_visualization():
    m_bits = 160
    num_nodes = 10
    
    nodes = []
    for i in range(num_nodes):
        node = ChordNode(f"192.168.1.{i}", 5000 + i, m_bits)
        nodes.append(node)
    
    nodes[0].create_ring()
    for i in range(1, num_nodes):
        nodes[i].join(nodes[0], init_fingers=True, transfer_data=False)
    
    for node in nodes:
        node.fix_fingers()
    
    visualize_chord_network(nodes, 'chord_network.png', show_fingers=False)
    visualize_chord_network(nodes, 'chord_network_with_fingers.png', show_fingers=True)


if __name__ == "__main__":
    test_chord_visualization()
