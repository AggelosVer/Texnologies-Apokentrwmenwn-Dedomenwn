from pastry_node import PastryNode
from pastry_visualizer import visualize_pastry_network, visualize_pastry_routing_table
import os

def run_topology_demo():
    print("Initializing Pastry nodes for topology visualization...")
    m_bits = 160
    num_nodes = 40
    
    # Create nodes
    nodes = []
    for i in range(num_nodes):
        # Using different ports to simulate different nodes
        node = PastryNode(f"127.0.0.1", 8000 + i, m_bits=m_bits)
        nodes.append(node)
    
    # Join nodes to form the network
    # First node creates the network
    for i in range(1, num_nodes):
        nodes[i].join(nodes[0])
    
    # Ensure directory exists
    os.makedirs('instances', exist_ok=True)
    
    print("Generating network topology plot: instances/pastry_network.png")
    visualize_pastry_network(nodes, 'instances/pastry_network.png')
    
    print("Generating detailed routing table plot for Node 0: instances/pastry_routing_table.png")
    visualize_pastry_routing_table(nodes[0], 'instances/pastry_routing_table.png')
    
    print("\nSuccess! Check the 'instances' folder for the images.")

if __name__ == "__main__":
    run_topology_demo()
