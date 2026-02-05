import pandas as pd
import json
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Set style for premium look
plt.style.use('seaborn-v0_8-muted')
sns.set_palette("husl")

def generate_performance_plots(csv_file='chord_vs_pastry_comparison.csv'):
    if not os.path.exists(csv_file):
        print(f"Error: {csv_file} not found. Please run compare_performance.py first.")
        return

    df = pd.read_csv(csv_file)
    
    # 1. Comparison of Basic Operations (Mean Time)
    # We'll filter for a specific parameter to make the bar chart readable
    # For build, we'll use nodes=20. For others, we'll use the mid-range parameter.
    
    operations = df['operation'].unique()
    
    fig, axes = plt.subplots(len(operations), 1, figsize=(12, 4 * len(operations)))
    
    for i, op in enumerate(operations):
        op_df = df[df['operation'] == op]
        
        # Pick the largest parameter for each operation for the main comparison
        max_param = op_df['parameter'].max()
        plot_df = op_df[op_df['parameter'] == max_param]
        
        sns.barplot(x='protocol', y='mean', data=plot_df, ax=axes[i], palette=['#2E86AB', '#A23B72'])
        axes[i].set_title(f'Mean Execution Time: {op.upper()} (Param={max_param})', fontsize=14, fontweight='bold')
        axes[i].set_ylabel('Time (seconds)', fontsize=12)
        axes[i].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('performance_comparison_bars.png', dpi=300)
    print("Saved performance_comparison_bars.png")

    # 2. Scaling Plots
    for op in operations:
        plt.figure(figsize=(10, 6))
        op_df = df[df['operation'] == op]
        
        for protocol in ['Chord', 'Pastry']:
            proto_df = op_df[op_df['protocol'] == protocol]
            plt.plot(proto_df['parameter'], proto_df['mean'], 'o-', label=protocol, linewidth=2.5, markersize=8)
            
        plt.title(f'Scaling: {op.capitalize()} Performance', fontsize=16, fontweight='bold')
        plt.xlabel('Parameter (Nodes/Items)', fontsize=12)
        plt.ylabel('Mean Time (s)', fontsize=12)
        plt.legend(fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.savefig(f'scaling_{op}.png', dpi=300)
        plt.close()
    
    print("Saved scaling plots for each operation.")

def generate_concurrency_plots(json_file='concurrency_benchmark_results.json'):
    if not os.path.exists(json_file):
        print(f"Error: {json_file} not found. Please run benchmark_concurrency.py first.")
        return

    with open(json_file, 'r') as f:
        data = json.load(f)
    
    df = pd.DataFrame(data)
    
    plt.figure(figsize=(10, 6))
    plt.plot(df['k'], df['total_time'], 'o-', color='#F39C12', linewidth=3, markersize=10)
    plt.title('Concurrent Movie Lookup: Total Time vs. K', fontsize=16, fontweight='bold')
    plt.xlabel('Number of Movies (K)', fontsize=12)
    plt.ylabel('Total Execution Time (s)', fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.savefig('concurrency_scaling.png', dpi=300)
    
    plt.figure(figsize=(10, 6))
    plt.plot(df['k'], df['avg_time'], 's-', color='#27AE60', linewidth=3, markersize=10)
    plt.title('Concurrent Movie Lookup: Avg Time per Lookup vs. K', fontsize=16, fontweight='bold')
    plt.xlabel('Number of Movies (K)', fontsize=12)
    plt.ylabel('Average Latency (s)', fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.savefig('concurrency_avg_latency.png', dpi=300)
    
    print("Saved concurrency plots.")

if __name__ == "__main__":
    generate_performance_plots()
    generate_concurrency_plots()
