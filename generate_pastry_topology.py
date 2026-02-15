from pastry_node import PastryNode
from pastry_visualizer import visualize_pastry_network, visualize_pastry_routing_table
import os

def run_topology_demo():
    print("Initializing Pastry nodes for topology visualization...")
    m_bits = 160
    num_nodes = 40
    
    nodes = []
    for i in range(num_nodes):
        
        node = PastryNode(f"127.0.0.1", 8000 + i, m_bits=m_bits)
        nodes.append(node)
    

    for i in range(1, num_nodes):
        nodes[i].join(nodes[0])
    
    os.makedirs('instances', exist_ok=True)
    
    print("Generating network topology plot: instances/pastry_network.png")
    visualize_pastry_network(nodes, 'instances/pastry_network.png')
    

if __name__ == "__main__":
    run_topology_demo()
