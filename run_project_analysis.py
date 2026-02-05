import subprocess
import sys
import os

def main():
    # Ensure instances directory exists
    os.makedirs('instances', exist_ok=True)

    scripts = [
        "chord_hop_analyzer.py",
        "pastry_hop_analyzer.py",
        "benchmark_chord.py",
        "benchmark_pastry.py",
        "compare_performance.py",
        "compare_hops.py",
        "plot_chord_hops.py",
        "plot_pastry_hops.py",
        "test_visualization.py"
    ]

    for script in scripts:
        print(f"Running {script}...")
        subprocess.run([sys.executable, script])

    print("\nAll scripts completed. Images saved in 'instances/'.")

if __name__ == "__main__":
    main()
