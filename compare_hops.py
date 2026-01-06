import json
import matplotlib.pyplot as plt
import numpy as np

def plot_comparison(chord_file='chord_hop_results.json', 
                   pastry_file='pastry_hop_results.json',
                   output_file='chord_vs_pastry_hops.png'):
    
    with open(chord_file, 'r') as f:
        chord_results = json.load(f)
    
    with open(pastry_file, 'r') as f:
        pastry_results = json.load(f)
    
    chord_sizes = [r['num_keys'] for r in chord_results]
    chord_mean = [r['mean_hops'] for r in chord_results]
    chord_min = [r['min_hops'] for r in chord_results]
    chord_max = [r['max_hops'] for r in chord_results]
    
    pastry_sizes = [r['num_keys'] for r in pastry_results]
    pastry_mean = [r['mean_hops'] for r in pastry_results]
    pastry_min = [r['min_hops'] for r in pastry_results]
    pastry_max = [r['max_hops'] for r in pastry_results]
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    axes[0, 0].plot(chord_sizes, chord_mean, 'o-', label='Chord Mean', 
                    linewidth=2, markersize=8, color='#2E86AB')
    axes[0, 0].plot(pastry_sizes, pastry_mean, 's-', label='Pastry Mean', 
                    linewidth=2, markersize=8, color='#A23B72')
    axes[0, 0].set_xlabel('Dataset Size (keys)', fontsize=11)
    axes[0, 0].set_ylabel('Mean Hops', fontsize=11)
    axes[0, 0].set_title('Mean Hop Count Comparison', fontsize=12, fontweight='bold')
    axes[0, 0].grid(True, alpha=0.3)
    axes[0, 0].legend(fontsize=10)
    
    axes[0, 1].plot(chord_sizes, chord_mean, 'o-', label='Chord Mean', 
                    linewidth=2, markersize=8, color='#2E86AB')
    axes[0, 1].plot(pastry_sizes, pastry_mean, 's-', label='Pastry Mean', 
                    linewidth=2, markersize=8, color='#A23B72')
    axes[0, 1].set_xscale('log')
    axes[0, 1].set_xlabel('Dataset Size (keys, log scale)', fontsize=11)
    axes[0, 1].set_ylabel('Mean Hops', fontsize=11)
    axes[0, 1].set_title('Mean Hop Count (Log Scale)', fontsize=12, fontweight='bold')
    axes[0, 1].grid(True, alpha=0.3, which='both')
    axes[0, 1].legend(fontsize=10)
    
    axes[1, 0].fill_between(chord_sizes, chord_min, chord_max, alpha=0.3, 
                            label='Chord Range', color='#2E86AB')
    axes[1, 0].fill_between(pastry_sizes, pastry_min, pastry_max, alpha=0.3, 
                            label='Pastry Range', color='#A23B72')
    axes[1, 0].plot(chord_sizes, chord_mean, 'o-', linewidth=2, markersize=6, color='#2E86AB')
    axes[1, 0].plot(pastry_sizes, pastry_mean, 's-', linewidth=2, markersize=6, color='#A23B72')
    axes[1, 0].set_xlabel('Dataset Size (keys)', fontsize=11)
    axes[1, 0].set_ylabel('Hops', fontsize=11)
    axes[1, 0].set_title('Hop Count Range Comparison', fontsize=12, fontweight='bold')
    axes[1, 0].grid(True, alpha=0.3)
    axes[1, 0].legend(fontsize=10)
    
    num_nodes = chord_results[0]['num_nodes']
    chord_theoretical = np.log2(num_nodes)
    pastry_theoretical = np.log(num_nodes) / np.log(2**4)
    
    x_pos = np.arange(len(chord_sizes))
    width = 0.35
    
    axes[1, 1].bar(x_pos - width/2, chord_mean, width, label='Chord', 
                   color='#2E86AB', alpha=0.7)
    axes[1, 1].bar(x_pos + width/2, pastry_mean, width, label='Pastry', 
                   color='#A23B72', alpha=0.7)
    axes[1, 1].axhline(y=chord_theoretical, color='#2E86AB', linestyle='--', 
                       linewidth=2, alpha=0.5, label=f'Chord Theoretical ({chord_theoretical:.2f})')
    axes[1, 1].axhline(y=pastry_theoretical, color='#A23B72', linestyle='--', 
                       linewidth=2, alpha=0.5, label=f'Pastry Theoretical ({pastry_theoretical:.2f})')
    axes[1, 1].set_xlabel('Dataset Size Index', fontsize=11)
    axes[1, 1].set_ylabel('Mean Hops', fontsize=11)
    axes[1, 1].set_title('Mean Hops vs Theoretical', fontsize=12, fontweight='bold')
    axes[1, 1].set_xticks(x_pos)
    axes[1, 1].set_xticklabels([f'{s//1000}k' for s in chord_sizes], rotation=45)
    axes[1, 1].grid(True, alpha=0.3, axis='y')
    axes[1, 1].legend(fontsize=9)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Saved comparison plot to {output_file}")
    
    print("\n=== Chord vs Pastry Hop Count Summary ===")
    print(f"Number of nodes: {num_nodes}")
    print(f"\nChord Theoretical Hops: {chord_theoretical:.2f}")
    print(f"Pastry Theoretical Hops: {pastry_theoretical:.2f}")
    print(f"\nDataset Size | Chord Mean | Pastry Mean | Difference")
    print("-" * 60)
    for i, size in enumerate(chord_sizes):
        diff = chord_mean[i] - pastry_mean[i]
        print(f"{size:>12} | {chord_mean[i]:>10.2f} | {pastry_mean[i]:>11.2f} | {diff:>+10.2f}")

if __name__ == "__main__":
    plot_comparison()
