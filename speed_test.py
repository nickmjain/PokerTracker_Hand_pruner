#!/usr/bin/env python3
"""
PT4 Pruner Speed Test

This script runs the pruner.py script multiple times with different optimization
configurations to measure and compare performance improvements.
"""

import argparse
import subprocess
import time
import statistics
import sys
import os
from datetime import datetime

def parse_arguments():
    """Parse command line arguments for speed testing"""
    parser = argparse.ArgumentParser(
        description="Speed test harness for PT4 Database Pruner. Runs multiple configurations and reports timing statistics."
    )

    parser.add_argument("--iterations", "-n", type=int, default=5,
                        help="Number of times to run each test (default: 5)")

    parser.add_argument("--hands", "-m", type=int, default=1000000,
                        help="Number of hands to process in each test (default: 1,000,000)")

    parser.add_argument("--type", "-t", choices=['cash', 'tourney'], default='cash',
                        help="Hand type to test (default: cash)")

    parser.add_argument("--days", "-d", type=int, default=365,
                        help="Inactivity threshold in days (default: 365)")

    parser.add_argument("--output", "-o", type=str,
                        default=f"pruner_speed_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                        help="Output file for results (default: pruner_speed_test_[timestamp].txt)")

    return parser.parse_args()

def extract_execution_time(output):
    """Extract execution time from pruner.py output"""
    for line in output.split('\n'):
        if "Total time elapsed:" in line:
            try:
                time_str = line.split("seconds")[0].split(":")[-1].strip()
                return float(time_str)
            except (IndexError, ValueError):
                pass
    return None

def run_test(name, cmd, iterations):
    """Run a test command multiple times and collect timing results"""
    print(f"\n=== TEST: {name} ===")
    print(f"Command: {cmd}")

    times = []
    for i in range(iterations):
        print(f"  Iteration {i+1}/{iterations}...", end="", flush=True)

        # Run the command
        start = time.time()
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            end = time.time()

            # First try to extract time from output
            extracted_time = extract_execution_time(result.stdout)

            # Fall back to measured time if extraction fails
            elapsed_time = extracted_time if extracted_time is not None else (end - start)

            times.append(elapsed_time)
            print(f" {elapsed_time:.2f}s")

        except Exception as e:
            print(f" ERROR: {e}")

    # Calculate statistics
    if not times:
        return None

    return {
        'name': name,
        'times': times,
        'min': min(times),
        'max': max(times),
        'mean': statistics.mean(times),
        'median': statistics.median(times) if len(times) > 0 else 0,
        'stdev': statistics.stdev(times) if len(times) > 1 else 0
    }

def print_table(results, baseline_mean):
    """Print results in a tabular format"""
    # Check if tabulate is available for pretty printing
    try:
        from tabulate import tabulate

        table_data = []
        for i, r in enumerate(results):
            speedup = baseline_mean / r['mean'] if r['mean'] > 0 else 0
            speedup_str = "1.00x" if i == 0 else f"{speedup:.2f}x"

            table_data.append([
                r['name'],
                f"{r['min']:.2f}s",
                f"{r['max']:.2f}s",
                f"{r['mean']:.2f}s",
                f"{r['median']:.2f}s",
                f"{r['stdev']:.2f}s",
                speedup_str
            ])

        headers = ["Configuration", "Min", "Max", "Mean", "Median", "Std Dev", "Speedup"]
        return tabulate(table_data, headers=headers, tablefmt="grid")

    except ImportError:
        # Fall back to simple table if tabulate isn't available
        result_str = "Configuration           Min      Max     Mean    Median   StdDev   Speedup\n"
        result_str += "-" * 80 + "\n"

        for i, r in enumerate(results):
            speedup = baseline_mean / r['mean'] if r['mean'] > 0 else 0
            speedup_str = "1.00x" if i == 0 else f"{speedup:.2f}x"

            result_str += f"{r['name']:<22} {r['min']:6.2f}s {r['max']:6.2f}s {r['mean']:6.2f}s "
            result_str += f"{r['median']:6.2f}s {r['stdev']:6.2f}s {speedup_str:>7}\n"

        return result_str

def main():
    args = parse_arguments()

    # Define test configurations
    tests = [
        {
            'name': 'Baseline',
            'args': ''
        },
        {
            'name': 'Parallel Processing',
            'args': '--parallel --chunks 4 --workers 4'
        },
        {
            'name': 'PostgreSQL Parallel',
            'args': '--pg-parallel'
        },
        {
            'name': 'Two-Phase Processing',
            'args': '--two-phase'
        },
        {
            'name': 'All Optimizations',
            'args': '--parallel --chunks 4 --workers 4 --pg-parallel --two-phase'
        }
    ]

    print(f"\n=== PT4 PRUNER PERFORMANCE TEST ===")
    print(f"Running {len(tests)} configurations, {args.iterations} iterations each")
    print(f"Testing with {args.hands:,} {args.type} hands, {args.days} days inactivity threshold")

    results = []

    for test in tests:
        # Construct full command
        cmd = f"python pruner.py --type {args.type} --limit {args.hands} --days {args.days} {test['args']}"

        # Run the test
        result = run_test(test['name'], cmd, args.iterations)
        if result:
            results.append(result)

    if not results:
        print("\nNo valid test results collected. Exiting.")
        return

    # Calculate speedup relative to baseline
    baseline_mean = results[0]['mean']

    # Print summary table
    print("\n=== PERFORMANCE SUMMARY ===")
    table = print_table(results, baseline_mean)
    print(table)

    # Save detailed results to file
    try:
        with open(args.output, 'w') as f:
            f.write("PT4 PRUNER PERFORMANCE TEST RESULTS\n")
            f.write(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Hands: {args.hands:,} {args.type}\n")
            f.write(f"Days: {args.days}\n")
            f.write(f"Iterations: {args.iterations}\n\n")

            f.write(table)
            f.write("\n\nRaw timing data (seconds):\n")

            for r in results:
                f.write(f"\n{r['name']}:\n")
                for i, t in enumerate(r['times']):
                    f.write(f"  Run {i+1}: {t:.2f}s\n")

        print(f"\nDetailed results saved to: {args.output}")
    except Exception as e:
        print(f"\nFailed to save results file: {e}")

    # Try to create a bar chart of the results
    try:
        import matplotlib.pyplot as plt

        # Extract data for plotting
        test_names = [r['name'] for r in results]
        mean_times = [r['mean'] for r in results]

        # Create the plot
        plt.figure(figsize=(12, 6))

        bars = plt.bar(test_names, mean_times, color='skyblue')
        plt.title('PT4 Pruner Performance Comparison')
        plt.xlabel('Configuration')
        plt.ylabel('Mean Execution Time (seconds)')
        plt.xticks(rotation=15, ha='right')
        plt.grid(axis='y', linestyle='--', alpha=0.7)

        # Add speedup annotations
        for i, bar in enumerate(bars):
            speedup = baseline_mean / mean_times[i] if mean_times[i] > 0 else 0
            speedup_text = f"({speedup:.2f}x)" if i > 0 else ""

            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                    f"{mean_times[i]:.2f}s\n{speedup_text}",
                    ha='center', va='bottom')

        # Save the plot
        plot_file = os.path.splitext(args.output)[0] + ".png"
        plt.tight_layout()
        plt.savefig(plot_file)
        print(f"Performance chart saved to: {plot_file}")

    except ImportError:
        print("\nMatplotlib not installed. Skipping chart generation.")
    except Exception as e:
        print(f"\nFailed to generate chart: {e}")

if __name__ == "__main__":
    main()