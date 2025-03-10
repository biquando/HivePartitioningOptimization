import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns


sizes = [1, 2, 4, 20]
algorithms = [1, 2]


def load_data_from_directories(
    base_path="saved_algorithm_reports/march_8_reports",
):
    """
    Load data from directories structured as <size>_algorithm<num>

    Returns:
        Dictionary of dataframes with structure {size: {algorithm: dataframe}}
    """
    all_data = {}
    algorithm_times = {}

    for size in sizes:
        size_data = {}
        algorithm_times[size] = {}
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

                        # Store the total algorithm time
                        if "total_algorithm_time" in data:
                            algorithm_times[size][alg] = data["total_algorithm_time"]

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

    return all_data, algorithm_times


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
                f"  Size {size} GB (Alg 1): Found {len(df)} data points across {len(df['table_name'].unique())} tables"
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
                -1 * avg_best_speedup
            )  # Convert negative percentages to positive speedup values
            print(
                f"  Average best speedup (Alg 1): {avg_best_speedup} → {abs(avg_best_speedup)}%"
            )
        else:
            best_speedups_alg1.append(0)  # No data for this size
            print(f"  Size {size} GB (Alg 1): No data available")

        # Process Algorithm 2 data
        if 2 in data_dict[size]:
            df = data_dict[size][2]
            print(
                f"  Size {size} GB (Alg 2): Found {len(df)} data points across {len(df['table_name'].unique())} tables"
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
                -1 * avg_best_speedup
            )  # Convert negative percentages to positive speedup values
            print(
                f"  Average best speedup (Alg 2): {avg_best_speedup} → {abs(avg_best_speedup)}%"
            )
        else:
            best_speedups_alg2.append(0)  # No data for this size
            print(f"  Size {size} GB (Alg 2): No data available")

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

        plt.xticks(x, [f"{s} GB" for s in sizes])
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


def plot_execution_time_by_size(algorithm_times):
    """
    Compare total algorithm execution times for algorithm 1 and 2
    across different data sizes using the total_algorithm_time field from the JSON files
    """
    sizes = sorted(algorithm_times.keys())
    exec_times_alg1 = []
    exec_times_alg2 = []

    print(
        f"Plotting total algorithm execution time by size with {len(sizes)} size points: {sizes}"
    )

    for size in sizes:
        # Get Algorithm 1 execution time
        if 1 in algorithm_times[size]:
            exec_time = algorithm_times[size][1]
            exec_times_alg1.append(exec_time)
            print(
                f"  Size {size} GB (Alg 1): Total algorithm execution time = {exec_time:.2f}s"
            )
        else:
            exec_times_alg1.append(0)  # No data for this size
            print(f"  Size {size} GB (Alg 1): No data available")

        # Get Algorithm 2 execution time
        if 2 in algorithm_times[size]:
            exec_time = algorithm_times[size][2]
            exec_times_alg2.append(exec_time)
            print(
                f"  Size {size} GB (Alg 2): Total algorithm execution time = {exec_time:.2f}s"
            )
        else:
            exec_times_alg2.append(0)  # No data for this size
            print(f"  Size {size} GB (Alg 2): No data available")

    # Create the plot
    plt.figure(figsize=(12, 6))

    if any(exec_times_alg1) or any(exec_times_alg2):
        # Plot line chart
        plt.plot(
            sizes,
            exec_times_alg1,
            "o-",
            label="Algorithm 1",
            color="#3498db",
            linewidth=2,
            markersize=8,
        )
        plt.plot(
            sizes,
            exec_times_alg2,
            "s-",
            label="Algorithm 2",
            color="#e74c3c",
            linewidth=2,
            markersize=8,
        )

        plt.xticks(sizes, [f"{s} GB" for s in sizes])
        plt.ylabel("Total Algorithm Execution Time (seconds)")
        plt.xlabel("Data Size")
        plt.title("Comparison of Total Algorithm Execution Times Across Data Sizes")
        plt.grid(True, linestyle="--", alpha=0.7)
        plt.legend()

        # Add value labels near points
        for i, (v1, v2) in enumerate(zip(exec_times_alg1, exec_times_alg2)):
            if v1 > 0:
                plt.text(
                    sizes[i],
                    v1 + 0.05 * max(exec_times_alg1),
                    f"{v1:.2f}s",
                    ha="center",
                    va="bottom",
                    fontsize=9,
                )
            if v2 > 0:
                plt.text(
                    sizes[i],
                    v2 - 0.05 * max(exec_times_alg2),
                    f"{v2:.2f}s",
                    ha="center",
                    va="top",
                    fontsize=9,
                )
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
        plt.title("Data Error: Total Algorithm Execution Times Across Data Sizes")

    plt.tight_layout()
    save_path = "plots/total_algorithm_execution_time.png"
    plt.savefig(save_path, dpi=300, bbox_inches="tight")
    plt.close()

    # Check if file was created
    if os.path.exists(save_path):
        print(f"  Plot saved to {save_path} ({os.path.getsize(save_path)/1024:.1f} KB)")
    else:
        print(f"  ERROR: Failed to save plot to {save_path}")

    return {"algorithm1": exec_times_alg1, "algorithm2": exec_times_alg2}


def plot_products_column_speedup(data_dict, size=1):
    """
    Plot 2: Bar chart of percentage speedup for each combination of columns for products table
    """
    if size not in data_dict or 1 not in data_dict[size]:
        print(f"No data found for size {size} GB and algorithm 1")
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

    # Use the original time difference percentages
    plt.bar(range(len(products_df)), products_df["time_difference_percent"])
    plt.xticks(
        range(len(products_df)),
        products_df["partition_columns"],
        rotation=45,
        ha="right",
    )
    plt.ylabel("Time Difference (%)")
    plt.xlabel("Column Combinations")
    plt.title(
        f"Performance Impact of Different Column Combinations for Products Table ({size} GB)"
    )
    plt.grid(axis="y", linestyle="--", alpha=0.7)

    # Add value labels on top of bars
    for i, value in enumerate(products_df["time_difference_percent"]):
        label = f"{value:.2f}%"
        y_pos = value - 2 if value < 0 else value + 1
        plt.text(
            i,
            y_pos,
            label,
            ha="center",
            va="bottom" if value >= 0 else "top",
            fontsize=9,
        )

    plt.tight_layout()
    plt.savefig(
        f"plots/products_column_speedup_{size}.png", dpi=300, bbox_inches="tight"
    )
    plt.close()


def plot_algorithm_comparison(data_dict, size=1):
    """
    Plot 3: Bar chart comparing speeds of algorithm 1 vs 2 for same tables
    """
    if size not in data_dict or 1 not in data_dict[size] or 2 not in data_dict[size]:
        print(f"Missing data for size {size} GB - need both algorithm 1 and 2")
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
                "algorithm1_best": -1 * best_speedup1,
                "algorithm2_best": -1 * best_speedup2,
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
    plt.title(f"Comparison of Best Speedup: Algorithm 1 vs Algorithm 2 ({size} GB)")
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


def plot_speedup_vs_cardinality(data_dict, table_name="orders", size=1, algorithm=1):
    """
    Scatter plot showing the relationship between cardinality product and
    speedup for different column combinations in a specific table
    """
    if size not in data_dict or algorithm not in data_dict[size]:
        print(f"No data found for size {size} GB and algorithm {algorithm}")
        return

    df = data_dict[size][algorithm]
    table_df = df[df["table_name"] == table_name].copy()

    if table_df.empty:
        print(f"No data found for '{table_name}' table")
        return

    # Filter to include only combinations with valid execution times
    table_df = table_df[table_df["execution_time"] != float("inf")]

    if len(table_df) < 2:  # Need at least baseline and one other point
        print(f"Not enough data points for {table_name} table")
        return

    # Create the plot
    plt.figure(figsize=(12, 8))

    # Create scatter plot with point sizes based on column count
    scatter = plt.scatter(
        table_df["cardinality_product"],
        -1 * table_df["time_difference_percent"],
        s=100 + 50 * table_df["column_count"],  # Size based on number of columns
        color="#3498db",
        alpha=0.7,
        edgecolors="black",
    )

    # Add labels for each point
    for i, row in table_df.iterrows():
        plt.annotate(
            row["partition_columns"],
            (row["cardinality_product"], -1 * row["time_difference_percent"]),
            xytext=(5, 5),
            textcoords="offset points",
            fontsize=8,
            bbox=dict(boxstyle="round,pad=0.3", fc="white", alpha=0.7),
        )

    plt.xscale(
        "log"
    )  # Log scale for cardinality which can span many orders of magnitude
    plt.xlabel("Cardinality Product (log scale)")
    plt.ylabel("Speedup (%)")
    plt.title(f"Speedup vs. Cardinality for {table_name} Table ({size} GB)")
    plt.grid(True, linestyle="--", alpha=0.6)

    # # Add a color bar to show column count
    # cbar = plt.colorbar(scatter)
    # cbar.set_label("Number of Partition Columns")

    # Add a trend line if enough points
    if len(table_df) >= 3:
        try:
            # Remove zero cardinality for log calculation
            trend_df = table_df[table_df["cardinality_product"] > 0].copy()
            if len(trend_df) >= 3:
                x = np.log10(trend_df["cardinality_product"])
                y = -1 * trend_df["time_difference_percent"]
                z = np.polyfit(x, y, 1)
                p = np.poly1d(z)

                # Generate x values for the trendline
                x_line = np.linspace(
                    np.log10(trend_df["cardinality_product"].min()),
                    np.log10(trend_df["cardinality_product"].max()),
                    100,
                )

                # Plot the trendline
                plt.plot(
                    10**x_line,
                    p(x_line),
                    "r--",
                    linewidth=2,
                    label=f"Trend: y = {z[0]:.2f}*log10(x) + {z[1]:.2f}",
                )
                plt.legend()
        except Exception as e:
            print(f"Could not calculate trend line: {e}")

    plt.tight_layout()
    save_path = f"plots/speedup_vs_cardinality_{table_name}_{size}gb.png"
    plt.savefig(save_path, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"✓ Generated speedup vs cardinality plot for {table_name}")


def create_speedup_heatmap(data_dict, size=1, algorithm=1):
    """
    Additional Plot: Heatmap visualization of speedup across tables and partition strategies
    """
    if size not in data_dict or algorithm not in data_dict[size]:
        print(f"No data found for size {size} GB and algorithm {algorithm}")
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
        f"Speedup Heatmap by Table and Partition Columns ({size} GB, Algorithm {algorithm})"
    )
    plt.tight_layout()
    plt.savefig(
        f"plots/speedup_heatmap_{size}gb_alg{algorithm}.png",
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def main():
    # Create the data directory if it doesn't exist
    os.makedirs("plots", exist_ok=True)

    # Load data from all directories
    print("Loading data from directories...")
    data_dict, algorithm_times = load_data_from_directories()

    if not data_dict:
        print("No data found in the specified directories.")
        return

    print(f"Found data for sizes: {list(data_dict.keys())}")

    # Generate the plots
    print("Generating plots...")

    # Plot 1: Best speedup by size
    plot_best_speedup_by_size(data_dict)
    print("✓ Generated best speedup by size chart")

    # Plot 2: Execution time by size (not speedup)
    plot_execution_time_by_size(algorithm_times)
    print("✓ Generated execution time by size chart")

    # Plot 3: Products column speedup
    for size in sizes:
        plot_products_column_speedup(data_dict, size=size)
    print("✓ Generated products column speedup charts")

    # Plot 4: Algorithm comparison
    for size in sizes:
        plot_algorithm_comparison(data_dict, size=size)
    print("✓ Generated algorithm comparison charts")

    # Plot 5:  Speedup vs cardinality for orders
    for size in sizes:
        for algorithm in algorithms:
            plot_speedup_vs_cardinality(
                data_dict, table_name="orders", size=size, algorithm=algorithm
            )
    for size in sizes:
        for algorithm in algorithms:
            plot_speedup_vs_cardinality(
                data_dict, table_name="products", size=size, algorithm=algorithm
            )
    print("✓ Generated speedup vs cardinality charts")

    # Additional visualization: Speedup heatmap
    for size in sizes:
        for algorithm in algorithms:
            create_speedup_heatmap(data_dict, size=size, algorithm=algorithm)
    print("✓ Generated speedup heatmaps")

    print("All plots have been successfully generated!")


if __name__ == "__main__":
    main()
