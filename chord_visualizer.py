import matplotlib.pyplot as plt
import matplotlib.patches as patches
import numpy as np
from typing import List
from chord_node import ChordNode


class ChordVisualizer:
    def __init__(self, nodes: List[ChordNode], m_bits: int = 160):
        self.nodes = nodes
        self.m_bits = m_bits
        self.ring_size = 2 ** m_bits
        
    def visualize_successor_graph(self, filename: str = 'chord_network.png', 
                                  show_fingers: bool = False, dpi: int = 100):
        fig, ax = plt.subplots(figsize=(12, 12), facecolor='white')
        ax.set_aspect('equal')
        ax.axis('off')
        
        center = np.array([0, 0])
        radius = 4.0
        
        node_positions = {}
        for node in self.nodes:
            angle = 2 * np.pi * (node.id / self.ring_size)
            x = center[0] + radius * np.cos(angle - np.pi/2)
            y = center[1] + radius * np.sin(angle - np.pi/2)
            node_positions[node.id] = (x, y)
        
        circle = plt.Circle(center, radius, fill=False, color='#e0e0e0', 
                           linewidth=2, linestyle='--')
        ax.add_patch(circle)
        
        if show_fingers:
            for node in self.nodes:
                start_pos = node_positions[node.id]
                for finger_node in node.finger_table:
                    if finger_node and finger_node.id != node.id:
                        end_pos = node_positions[finger_node.id]
                        ax.plot([start_pos[0], end_pos[0]], 
                               [start_pos[1], end_pos[1]], 
                               color='#90caf9', alpha=0.3, linewidth=0.8)
        
        for node in self.nodes:
            if node.successor:
                start_pos = node_positions[node.id]
                end_pos = node_positions[node.successor.id]
                
                dx = end_pos[0] - start_pos[0]
                dy = end_pos[1] - start_pos[1]
                
                ax.annotate('', xy=end_pos, xytext=start_pos,
                           arrowprops=dict(arrowstyle='->', color='#42a5f5', 
                                         lw=2, mutation_scale=20))
        
        for node in self.nodes:
            pos = node_positions[node.id]
            circle = plt.Circle(pos, 0.25, color='#1976d2', zorder=10)
            ax.add_patch(circle)
            
            ax.text(pos[0], pos[1], str(node.id % 1000), 
                   ha='center', va='center', color='white', 
                   fontsize=9, weight='bold', zorder=11)
            
            label_radius = radius + 0.5
            angle = 2 * np.pi * (node.id / self.ring_size)
            label_x = center[0] + label_radius * np.cos(angle - np.pi/2)
            label_y = center[1] + label_radius * np.sin(angle - np.pi/2)
            ax.text(label_x, label_y, f'{node.address}', 
                   ha='center', va='center', fontsize=7, color='#424242')
        
        ax.set_xlim(-6, 6)
        ax.set_ylim(-6, 6)
        
        plt.tight_layout()
        plt.savefig(filename, dpi=dpi, bbox_inches='tight', facecolor='white')
        plt.close()


def visualize_chord_network(nodes: List[ChordNode], filename: str = 'chord_network.png',
                           show_fingers: bool = False, m_bits: int = 160):
    visualizer = ChordVisualizer(nodes, m_bits)
    visualizer.visualize_successor_graph(filename, show_fingers)
