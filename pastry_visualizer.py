import matplotlib.pyplot as plt
import matplotlib.patches as patches
import numpy as np
from typing import List
from pastry_node import PastryNode


class PastryVisualizer:
    def __init__(self, nodes: List[PastryNode], m_bits: int = 160):
        self.nodes = nodes
        self.m_bits = m_bits
        self.ring_size = 2 ** m_bits
        
    def visualize_routing_connections(self, filename: str = 'pastry_network.png', 
                                     show_leaf_set: bool = True,
                                     show_routing_table: bool = True,
                                     show_neighborhood: bool = False,
                                     dpi: int = 100):
        fig, ax = plt.subplots(figsize=(14, 14), facecolor='white')
        ax.set_aspect('equal')
        ax.axis('off')
        
        center = np.array([0, 0])
        radius = 5.0
        
        node_positions = {}
        for node in self.nodes:
            angle = 2 * np.pi * (node.id / self.ring_size)
            x = center[0] + radius * np.cos(angle - np.pi/2)
            y = center[1] + radius * np.sin(angle - np.pi/2)
            node_positions[node.id] = (x, y)
        
        circle = plt.Circle(center, radius, fill=False, color='#e0e0e0', 
                           linewidth=2, linestyle='--')
        ax.add_patch(circle)
        
        if show_neighborhood:
            for node in self.nodes:
                start_pos = node_positions[node.id]
                for neighbor in node.neighborhood_set:
                    if neighbor.id in node_positions:
                        end_pos = node_positions[neighbor.id]
                        ax.plot([start_pos[0], end_pos[0]], 
                               [start_pos[1], end_pos[1]], 
                               color='#ce93d8', alpha=0.2, linewidth=0.6)
        
        if show_routing_table:
            for node in self.nodes:
                start_pos = node_positions[node.id]
                for row in node.routing_table.values():
                    for rt_node in row.values():
                        if rt_node.id in node_positions:
                            end_pos = node_positions[rt_node.id]
                            ax.plot([start_pos[0], end_pos[0]], 
                                   [start_pos[1], end_pos[1]], 
                                   color='#66bb6a', alpha=0.4, linewidth=1.2)
        
        if show_leaf_set:
            for node in self.nodes:
                start_pos = node_positions[node.id]
                for leaf_node in node.get_leaf_set():
                    if leaf_node.id in node_positions:
                        end_pos = node_positions[leaf_node.id]
                        
                        dx = end_pos[0] - start_pos[0]
                        dy = end_pos[1] - start_pos[1]
                        
                        ax.annotate('', xy=end_pos, xytext=start_pos,
                                   arrowprops=dict(arrowstyle='->', color='#ef5350', 
                                                 lw=1.5, mutation_scale=15, alpha=0.7))
        
        for node in self.nodes:
            pos = node_positions[node.id]
            circle = plt.Circle(pos, 0.3, color='#1976d2', zorder=10)
            ax.add_patch(circle)
            
            ax.text(pos[0], pos[1], str(node.id % 1000), 
                   ha='center', va='center', color='white', 
                   fontsize=9, weight='bold', zorder=11)
            
            label_radius = radius + 0.6
            angle = 2 * np.pi * (node.id / self.ring_size)
            label_x = center[0] + label_radius * np.cos(angle - np.pi/2)
            label_y = center[1] + label_radius * np.sin(angle - np.pi/2)
            ax.text(label_x, label_y, f'{node.address}', 
                   ha='center', va='center', fontsize=7, color='#424242')
        
        ax.set_xlim(-7, 7)
        ax.set_ylim(-7, 7)
        
        plt.tight_layout()
        plt.savefig(filename, dpi=dpi, bbox_inches='tight', facecolor='white')
        plt.close()
    
    def visualize_routing_table_detail(self, node: PastryNode, filename: str = 'pastry_routing_table.png'):
        fig, ax = plt.subplots(figsize=(12, 8), facecolor='white')
        ax.axis('off')
        
        num_rows = node.num_rows
        num_cols = node.base
        
        cell_width = 0.8
        cell_height = 0.5
        start_x = 1
        start_y = num_rows * cell_height + 1
        
        ax.text(start_x - 0.5, start_y + 0.5, f'Routing Table for {node.address}',
               fontsize=14, weight='bold', color='#1976d2')
        
        for row in range(num_rows):
            y = start_y - row * cell_height
            ax.text(start_x - 0.5, y - cell_height/2, f'{row}',
                   ha='center', va='center', fontsize=10, weight='bold')
            
            for col in range(num_cols):
                x = start_x + col * cell_width
                
                rect = patches.Rectangle((x, y - cell_height), cell_width, cell_height,
                                        linewidth=1, edgecolor='#757575', facecolor='#f5f5f5')
                ax.add_patch(rect)
                
                ax.text(x + cell_width/2, y - 0.1, f'{col:X}',
                       ha='center', va='top', fontsize=8, color='#9e9e9e')
                
                if row in node.routing_table and col in node.routing_table[row]:
                    rt_node = node.routing_table[row][col]
                    node_id_short = str(rt_node.id % 10000)
                    
                    rect.set_facecolor('#4caf50')
                    rect.set_alpha(0.3)
                    
                    ax.text(x + cell_width/2, y - cell_height/2, node_id_short,
                           ha='center', va='center', fontsize=8, weight='bold', color='#2e7d32')
        
        leaf_y = start_y - (num_rows + 1) * cell_height
        ax.text(start_x - 0.5, leaf_y, 'Leaf Set:',
               fontsize=11, weight='bold', color='#1976d2')
        
        leaf_x = start_x
        for i, leaf_node in enumerate(node.get_leaf_set()):
            rect = patches.Rectangle((leaf_x, leaf_y - 0.4), 0.6, 0.35,
                                    linewidth=1, edgecolor='#ef5350', facecolor='#ffebee')
            ax.add_patch(rect)
            ax.text(leaf_x + 0.3, leaf_y - 0.22, str(leaf_node.id % 1000),
                   ha='center', va='center', fontsize=8, color='#c62828')
            leaf_x += 0.7
        
        ax.set_xlim(start_x - 1, start_x + num_cols * cell_width + 1)
        ax.set_ylim(leaf_y - 1, start_y + 1)
        
        plt.tight_layout()
        plt.savefig(filename, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()


def visualize_pastry_network(nodes: List[PastryNode], filename: str = 'pastry_network.png',
                             show_leaf_set: bool = True,
                             show_routing_table: bool = True,
                             show_neighborhood: bool = False,
                             m_bits: int = 160):
    visualizer = PastryVisualizer(nodes, m_bits)
    visualizer.visualize_routing_connections(filename, show_leaf_set, show_routing_table, show_neighborhood)


def visualize_pastry_routing_table(node: PastryNode, filename: str = 'pastry_routing_table.png'):
    visualizer = PastryVisualizer([node], node.m_bits)
    visualizer.visualize_routing_table_detail(node, filename)
