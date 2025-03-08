import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from pathlib import Path

sizes = [25, 50, 100, 500]
algorithms = [1, 2]


def load_data_from_directories(base_path="saved_algorithm_reports/march_7_reports"):
    """
    Load data from directories structured as <size>_algorithm<num>

    Returns:
        Dictionary of dataframes with structure {size: {algorithm: dataframe}}
    """
    all_data = {}

    for size in sizes:
        size_data = {}
        for alg in algorithms:
            dir_path = os.path.join(base_path, f"algorithm_{alg}_{size}")

            # Skip if directory doesn't exist
            if not os.path.exists(dir_path):
                continue

            # Process all JSON files in the directory
            table_data = []
            for filename in os.listdir(dir_path):
                if filename.endswith(".json"):
                    file_path = os.path.join(dir_path, filename)
                    with open(file_path, "r") as f:
                        data = json.load(f)

                        # Extract relevant information
                        table_name = data.get(
                            "table_name", filename.replace("_report.json", "")
                        )
                        algorithm = data.get("algorithm", f"algorithm_{alg}")
                        data_size = data.get("data_size_MiB", size)

                        # Process results
                        for result in data.get("results", []):
                            partition_columns = result.get("partition_columns", [])
                            execution_time = result.get(
                                "execution_time_seconds", float("inf")
                            )
                            time_diff_percent = result.get("time_difference_percent", 0)
                            cardinality = result.get("cardinality_product", 0)

                            # Skip infinite execution times
                            if execution_time == "inf" or execution_time == float(
                                "inf"
                            ):
                                continue

                            table_data.append(
                                {
                                    "table_name": table_name,
                                    "partition_columns": (
                                        ",".join(partition_columns)
                                        if partition_columns
                                        else "None"
                                    ),
                                    "execution_time": execution_time,
                                    "time_difference_percent": time_diff_percent,
                                    "cardinality_product": cardinality,
                                    "column_count": len(partition_columns),
                                }
                            )

            if table_data:
                size_data[alg] = pd.DataFrame(table_data)

        if size_data:
            all_data[size] = size_data

    return all_data


def plot_best_speedup_by_size(data_dict):
    """
    Plot 1: Graph comparing the average percent speedup of the best table for multiple data sizes,
    with comparison between algorithm 1 and algorithm 2
    """
    sizes = sorted(data_dict.keys())
    best_speedups_alg1 = []
    best_speedups_alg2 = []

    print(f"Plotting best speedup by size with {len(sizes)} size points: {sizes}")

    for size in sizes:
        # Process Algorithm 1 data
        if 1 in data_dict[size]:
            df = data_dict[size][1]
            print(
                f"  Size {size} MiB (Alg 1): Found {len(df)} data points across {len(df['table_name'].unique())} tables"
            )

            # Group by table and find best speedup for each table
            best_by_table = (
                df.groupby("table_name")["time_difference_percent"].min().reset_index()
            )
            print(
                f"  Best speedups per table (Alg 1): {dict(zip(best_by_table['table_name'], best_by_table['time_difference_percent']))}"
            )

            # Calculate average of best speedups across all tables
            avg_best_speedup = best_by_table["time_difference_percent"].mean()
            best_speedups_alg1.append(
                abs(avg_best_speedup)
            )  # Convert to positive for visualization
            print(
                f"  Average best speedup (Alg 1): {avg_best_speedup} → {abs(avg_best_speedup)}%"
            )
        else:
            best_speedups_alg1.append(0)  # No data for this size
            print(f"  Size {size} MiB (Alg 1): No data available")

        # Process Algorithm 2 data
        if 2 in data_dict[size]:
            df = data_dict[size][2]
            print(
                f"  Size {size} MiB (Alg 2): Found {len(df)} data points across {len(df['table_name'].unique())} tables"
            )

            # Group by table and find best speedup for each table
            best_by_table = (
                df.groupby("table_name")["time_difference_percent"].min().reset_index()
            )
            print(
                f"  Best speedups per table (Alg 2): {dict(zip(best_by_table['table_name'], best_by_table['time_difference_percent']))}"
            )

            # Calculate average of best speedups across all tables
            avg_best_speedup = best_by_table["time_difference_percent"].mean()
            best_speedups_alg2.append(
                abs(avg_best_speedup)
            )  # Convert to positive for visualization
            print(
                f"  Average best speedup (Alg 2): {avg_best_speedup} → {abs(avg_best_speedup)}%"
            )
        else:
            best_speedups_alg2.append(0)  # No data for this size
            print(f"  Size {size} MiB (Alg 2): No data available")

    # Create the plot
    plt.figure(figsize=(12, 6))

    if any(best_speedups_alg1) or any(best_speedups_alg2):
        # Set up bar positions
        x = np.arange(len(sizes))
        width = 0.35

        # Create bars for both algorithms
        bars1 = plt.bar(
            x - width / 2,
            best_speedups_alg1,
            width,
            label="Algorithm 1",
            color="#3498db",
        )
        bars2 = plt.bar(
            x + width / 2,
            best_speedups_alg2,
            width,
            label="Algorithm 2",
            color="#e74c3c",
        )

        plt.xticks(x, [f"{s} MiB" for s in sizes])
        plt.ylabel("Average Best Speedup (%)")
        plt.xlabel("Data Size")
        plt.title(
            "Comparison of Average Best Speedup Across Tables by Data Size and Algorithm"
        )
        plt.grid(axis="y", linestyle="--", alpha=0.7)
        plt.legend()

        # Add value labels on top of bars
        for i, v in enumerate(best_speedups_alg1):
            if v > 0:  # Only add label if there's a value
                plt.text(i - width / 2, v + 0.5, f"{v:.2f}%", ha="center", fontsize=9)

        for i, v in enumerate(best_speedups_alg2):
            if v > 0:  # Only add label if there's a value
                plt.text(i + width / 2, v + 0.5, f"{v:.2f}%", ha="center", fontsize=9)
    else:
        plt.text(
            0.5,
            0.5,
            "No valid data found for plotting",
            ha="center",
            va="center",
            fontsize=12,
            transform=plt.gca().transAxes,
        )
        plt.title("Data Error: Average Best Speedup Across Tables by Data Size")

    plt.tight_layout()
    save_path = "plots/best_speedup_by_size.png"
    plt.savefig(save_path, dpi=300, bbox_inches="tight")
    plt.close()

    # Check if file was created
    if os.path.exists(save_path):
        print(f"  Plot saved to {save_path} ({os.path.getsize(save_path)/1024:.1f} KB)")
    else:
        print(f"  ERROR: Failed to save plot to {save_path}")

    return {"algorithm1": best_speedups_alg1, "algorithm2": best_speedups_alg2}


def plot_products_column_speedup(data_dict, size=25):
    """
    Plot 2: Bar chart of percentage speedup for each combination of columns for products table
    """
    if size not in data_dict or 1 not in data_dict[size]:
        print(f"No data found for size {size} MiB and algorithm 1")
        return

    df = data_dict[size][1]
    products_df = df[df["table_name"] == "products"].copy()

    if products_df.empty:
        print("No data found for 'products' table")
        return

    # Filter to include only combinations with speedup and limit to top combinations
    products_df = products_df[
        products_df["time_difference_percent"] > -100
    ]  # Exclude extreme outliers
    products_df = products_df.sort_values("time_difference_percent")

    # Keep only top combinations (exclude baseline/empty partition)
    products_df = products_df[products_df["partition_columns"] != "None"]

    if len(products_df) < 3:
        print(
            f"Not enough column combinations with speedup for products table (found {len(products_df)})"
        )
        return

    # Create the plot
    plt.figure(figsize=(12, 7))

    # Use absolute values for better visualization
    abs_speedup = products_df["time_difference_percent"].abs()
    colors = plt.cm.viridis(np.linspace(0.2, 0.8, len(products_df)))

    bars = plt.bar(range(len(products_df)), abs_speedup, color=colors)
    plt.xticks(
        range(len(products_df)),
        products_df["partition_columns"],
        rotation=45,
        ha="right",
    )
    plt.ylabel(
        "Speedup (%)"
        if all(products_df["time_difference_percent"] < 0)
        else "Time Difference (%)"
    )
    plt.xlabel("Column Combinations")
    plt.title("Performance Impact of Different Column Combinations for Products Table")
    plt.grid(axis="y", linestyle="--", alpha=0.7)

    # Add value labels on top of bars
    for bar, value in zip(bars, products_df["time_difference_percent"]):
        label = f"{abs(value):.2f}%" if value < 0 else f"+{value:.2f}%"
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 1,
            label,
            ha="center",
            va="bottom",
            fontsize=9,
        )

    plt.tight_layout()
    plt.savefig(
        f"plots/products_column_speedup_{size}.png", dpi=300, bbox_inches="tight"
    )
    plt.close()


def plot_algorithm_comparison(data_dict, size=25):
    """
    Plot 3: Bar chart comparing speeds of algorithm 1 vs 2 for same tables
    """
    if size not in data_dict or 1 not in data_dict[size] or 2 not in data_dict[size]:
        print(f"Missing data for size {size} MiB - need both algorithm 1 and 2")
        return

    df1 = data_dict[size][1]
    df2 = data_dict[size][2]

    # Find common tables between both algorithms
    common_tables = set(df1["table_name"].unique()).intersection(
        set(df2["table_name"].unique())
    )

    if not common_tables:
        print("No common tables found between algorithms")
        return

    # For each table, find the best speedup from each algorithm
    comparison_data = []

    for table in common_tables:
        table_df1 = df1[df1["table_name"] == table]
        table_df2 = df2[df2["table_name"] == table]

        best_speedup1 = table_df1["time_difference_percent"].min()
        best_speedup2 = table_df2["time_difference_percent"].min()

        comparison_data.append(
            {
                "table": table,
                "algorithm1_best": abs(best_speedup1),
                "algorithm2_best": abs(best_speedup2),
            }
        )

    comparison_df = pd.DataFrame(comparison_data)
    comparison_df = comparison_df.sort_values("table")

    # Create the plot
    plt.figure(figsize=(12, 7))

    x = np.arange(len(comparison_df))
    width = 0.35

    plt.bar(
        x - width / 2,
        comparison_df["algorithm1_best"],
        width,
        label="Algorithm 1",
        color="#3498db",
    )
    plt.bar(
        x + width / 2,
        comparison_df["algorithm2_best"],
        width,
        label="Algorithm 2",
        color="#e74c3c",
    )

    plt.xlabel("Table")
    plt.ylabel("Best Speedup (%)")
    plt.title(f"Comparison of Best Speedup: Algorithm 1 vs Algorithm 2 ({size} MiB)")
    plt.xticks(x, comparison_df["table"])
    plt.legend()
    plt.grid(axis="y", linestyle="--", alpha=0.7)

    # Add value labels on top of bars
    for i, (v1, v2) in enumerate(
        zip(comparison_df["algorithm1_best"], comparison_df["algorithm2_best"])
    ):
        plt.text(
            i - width / 2, v1 + 1, f"{v1:.1f}%", ha="center", va="bottom", fontsize=9
        )
        plt.text(
            i + width / 2, v2 + 1, f"{v2:.1f}%", ha="center", va="bottom", fontsize=9
        )

    plt.tight_layout()
    plt.savefig("plots/algorithm_comparison.png", dpi=300, bbox_inches="tight")
    plt.close()


def create_speedup_heatmap(data_dict, size=25, algorithm=1):
    """
    Additional Plot: Heatmap visualization of speedup across tables and partition strategies
    """
    if size not in data_dict or algorithm not in data_dict[size]:
        print(f"No data found for size {size} MiB and algorithm {algorithm}")
        return

    df = data_dict[size][algorithm]

    # Keep only rows with finite execution times
    df = df[df["execution_time"] != float("inf")]

    # Create a pivot table for the heatmap
    pivot_df = df.pivot_table(
        index="table_name",
        columns="partition_columns",
        values="time_difference_percent",
        aggfunc="first",  # Just take the first value if duplicates exist
    ).fillna(0)

    # Sort columns by average performance impact
    col_means = pivot_df.mean()
    sorted_cols = col_means.sort_values().index
    pivot_df = pivot_df[sorted_cols]

    # Create the heatmap
    plt.figure(figsize=(14, 10))
    sns.heatmap(
        pivot_df, annot=True, cmap="RdYlGn_r", center=0, fmt=".1f", linewidths=0.5
    )
    plt.title(
        f"Speedup Heatmap by Table and Partition Columns ({size} MiB, Algorithm {algorithm})"
    )
    plt.tight_layout()
    plt.savefig(
        f"plots/speedup_heatmap_{size}mb_alg{algorithm}.png",
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def main():
    # Create the data directory if it doesn't exist
    os.makedirs("plots", exist_ok=True)

    # Load data from all directories
    print("Loading data from directories...")
    data_dict = load_data_from_directories()

    if not data_dict:
        print("No data found in the specified directories.")
        return

    print(f"Found data for sizes: {list(data_dict.keys())}")

    # Generate the requested plots
    print("Generating plots...")

    # Plot 1: Best speedup by size
    plot_best_speedup_by_size(data_dict)
    print("✓ Generated best speedup by size chart")

    # Plot 2: Products column speedup
    for size in sizes:
        plot_products_column_speedup(data_dict, size=size)
        print("✓ Generated products column speedup chart")

    # Plot 3: Algorithm comparison
    for size in sizes:
        plot_algorithm_comparison(data_dict, size=size)
        print("✓ Generated algorithm comparison chart")

    # Additional visualization: Speedup heatmap
    for size in sizes:
        for algorithm in algorithms:
            create_speedup_heatmap(data_dict, size=size, algorithm=algorithm)
            print("✓ Generated speedup heatmap")

    print("All plots have been successfully generated!")


if __name__ == "__main__":
    main()
