import os
import shutil

def main():
    # Files to delete
    files_to_remove = [
        "chord_hop_results.json",
        "pastry_hop_results.json",
        "concurrency_benchmark_results.json",
        "chord_benchmark_results.csv",
        "pastry_benchmark_results.csv",
        "chord_vs_pastry_comparison.csv",
        "movie_dht_mappings.json",
        "performance_comparison_bars.png",
        "concurrency_scaling.png",
        "concurrency_avg_latency.png",
        "scaling_node_join.png",
        "scaling_lookup.png",
        "scaling_build.png",
        "scaling_insert.png",
        "scaling_delete.png",
        "scaling_node_leave.png",
        "scaling_update.png",
        "chord_vs_pastry_hops.png"
    ]
    
    # Directory to delete
    directory_to_remove = "instances"

    print("Cleaning project outputs...")

    # Delete files
    for file in files_to_remove:
        if os.path.exists(file):
            try:
                os.remove(file)
                print(f"Removed: {file}")
            except Exception as e:
                print(f"Error removing {file}: {e}")

    # Delete directory
    if os.path.exists(directory_to_remove):
        try:
            shutil.rmtree(directory_to_remove)
            print(f"Removed directory: {directory_to_remove}")
        except Exception as e:
            print(f"Error removing {directory_to_remove}: {e}")

    print("\nCleanup complete.")

if __name__ == "__main__":
    main()
