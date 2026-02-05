import json
import matplotlib.pyplot as plt
import numpy as np

import os

def plot_hop_counts(results_file='pastry_hop_results.json', output_file='instances/pastry_hops.png'):

    os.makedirs('instances', exist_ok=True)
    with open(results_file, 'r') as f:
        results = json.load(f)
    
    sizes = [r['num_keys'] for r in results]
    mean_hops = [r['mean_hops'] for r in results]
    min_hops = [r['min_hops'] for r in results]
    max_hops = [r['max_hops'] for r in results]
    median_hops = [r['median_hops'] for r in results]
    
    plt.figure(figsize=(12, 8))
    
    plt.subplot(2, 1, 1)
    plt.plot(sizes, mean_hops, 'o-', label='Mean', linewidth=2, markersize=8, color='#A23B72')
    plt.plot(sizes, median_hops, 's-', label='Median', linewidth=2, markersize=6, color='#F18F01')
    plt.fill_between(sizes, min_hops, max_hops, alpha=0.2, label='Min-Max Range', color='#A23B72')
    plt.xlabel('Dataset Size (keys)', fontsize=12)
    plt.ylabel('Number of Hops', fontsize=12)
    plt.title('Pastry Lookup Hop Count vs Dataset Size', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=10)
    
    plt.subplot(2, 1, 2)
    plt.plot(sizes, mean_hops, 'o-', linewidth=2, markersize=8, color='#A23B72')
    plt.xscale('log')
    plt.xlabel('Dataset Size (keys, log scale)', fontsize=12)
    plt.ylabel('Number of Hops', fontsize=12)
    plt.title('Pastry Lookup Hop Count (Log Scale)', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3, which='both')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Saved plot to {output_file}")
    
    theoretical_hops = [np.log(20) / np.log(2**4) for _ in sizes]
    
    plt.figure(figsize=(10, 6))
    plt.plot(sizes, mean_hops, 'o-', label='Actual Mean Hops', linewidth=2, markersize=8, color='#A23B72')
    plt.axhline(y=theoretical_hops[0], color='r', linestyle='--', linewidth=2, 
                label=f'Theoretical O(log_b N) = {theoretical_hops[0]:.2f}')
    plt.xlabel('Dataset Size (keys)', fontsize=12)
    plt.ylabel('Number of Hops', fontsize=12)
    plt.title('Pastry Hop Count: Actual vs Theoretical', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=10)
    plt.tight_layout()
    plt.savefig('instances/pastry_hops_comparison.png', dpi=300, bbox_inches='tight')
    print(f"Saved comparison plot to pastry_hops_comparison.png")

if __name__ == "__main__":
    plot_hop_counts()
