import os
import json
from datetime import datetime


def write_consolidated_report(all_results, algorithm_name, metadata=None):
    """Writes the results for all tables to a single directory.

    Args:
        all_results: Dictionary mapping table names to their results
        algorithm_name: Name of the algorithm used
        metadata: Dictionary containing run metadata (data_size, total_time, etc.)
    """
    # Generate a timestamp for the folder name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Create the directory with timestamp
    report_dir = f"algorithm_reports/{algorithm_name.lower()}_{timestamp}"
    os.makedirs(report_dir, exist_ok=True)

    # Write metadata summary file
    summary_filename = f"{report_dir}/summary.txt"
    with open(summary_filename, "w") as file:
        file.write("Algorithm Run Summary\n")
        file.write("===================\n\n")

        # Write metadata if provided
        if metadata:
            file.write("Run Configuration:\n")
            file.write("-----------------\n")
            file.write(f"Algorithm: {algorithm_name}\n")
            file.write(f"Data Size: {metadata.get('data_size', 'N/A')} MiB\n")
            file.write(f"Tables Processed: {', '.join(all_results.keys())}\n")
            file.write(
                f"Total Execution Time: {metadata.get('total_time', 'N/A'):.2f} seconds\n"
            )

        # Only write initial query time if it exists in metadata
        if "initial_query_time" in metadata:
            file.write(
                f"Initial Query Time: {metadata['initial_query_time']:.2f} seconds\n"
            )

        # Write summary statistics for each table
        file.write("Per-Table Summary:\n")
        file.write("----------------\n")
        for table_name, results in all_results.items():
            file.write(f"\n{table_name}:\n")

            # Get baseline and best times
            baseline_time = next(
                (time for columns, time, _ in results if columns == []), None
            )
            best_result = min(
                results, key=lambda x: x[1] if x[1] != float("inf") else float("inf")
            )

            file.write(f"  Baseline Time: {baseline_time:.2f} seconds\n")
            file.write(f"  Best Time: {best_result[1]:.2f} seconds\n")
            file.write(
                f"  Best Partition: {best_result[0] if best_result[0] else 'None'}\n"
            )
            if baseline_time and best_result[1] != float("inf"):
                improvement = ((baseline_time - best_result[1]) / baseline_time) * 100
                file.write(f"  Improvement: {improvement:.2f}%\n")

    # Write detailed results for each table as JSON
    for table_name, results in all_results.items():
        # Create JSON report
        json_report_filename = f"{report_dir}/{table_name}_report.json"

        # Count successfully tested column groups (finite execution time)
        column_groups_tested = sum(1 for _, time, _ in results if time != float("inf"))

        # Create JSON object
        report_data = {
            "table_name": table_name,
            "algorithm": algorithm_name,
            "data_size_MiB": metadata.get("data_size", "N/A") if metadata else "N/A",
            "timestamp": timestamp,
            "total_algorithm_time": (
                metadata.get("total_time", "N/A") if metadata else "N/A"
            ),
            "column_groups_tested": column_groups_tested,
            "results": [],
        }

        # Find the baseline execution time
        no_partition_time = next(
            (time for columns, time, _ in results if columns == []), None
        )

        # Add results
        for columns, time, cardinality_product in results:
            if no_partition_time is not None and time != float("inf"):
                time_diff_percent = (
                    (time - no_partition_time) / no_partition_time
                ) * 100
            else:
                time_diff_percent = None

            result_item = {
                "partition_columns": columns,
                "execution_time_seconds": (
                    round(time, 4) if time != float("inf") else "inf"
                ),
                "cardinality_product": cardinality_product,
                "time_difference_percent": (
                    round(time_diff_percent, 2)
                    if time_diff_percent is not None
                    else None
                ),
            }
            report_data["results"].append(result_item)

        # Write JSON file
        with open(json_report_filename, "w") as file:
            json.dump(report_data, file, indent=2)

        # Also keep the text version for backward compatibility
        report_filename = f"{report_dir}/{table_name}_report.txt"
        with open(report_filename, "w") as file:
            # Write table metadata
            file.write(f"Detailed Results for {table_name}\n")
            file.write("=" * (20 + len(table_name)) + "\n\n")

            if metadata:
                file.write(f"Algorithm: {algorithm_name}\n")
                file.write(f"Data Size: {metadata.get('data_size', 'N/A')} MiB\n")
                file.write(f"Run Timestamp: {timestamp}\n")
                file.write(
                    f"Total Algorithm Time: {metadata.get('total_time', 'N/A'):.2f} seconds\n"
                )
                file.write(f"Column Groups Tested: {column_groups_tested}\n\n")

            # Write the detailed results
            file.write(
                f"{'Partition Columns':<30} {'Execution Time (s)':<20} {'Cardinality Product':<25} {'Time Difference (%)'}\n"
            )
            file.write(f"{'-' * 80}\n")

            for columns, time, cardinality_product in results:
                if no_partition_time is not None and time != float("inf"):
                    time_diff_percent = (
                        (time - no_partition_time) / no_partition_time
                    ) * 100
                else:
                    time_diff_percent = None

                file.write(
                    f"{str(columns):<30} {round(time, 4) if time != float('inf') else 'inf':<20} {cardinality_product:<25} {time_diff_percent if time_diff_percent is None else round(time_diff_percent, 2)}\n"
                )

        print(f"Results for table {table_name} have been written to:")
        print(f"  - Text report: {report_filename}")
        print(f"  - JSON report: {json_report_filename}")

    print(f"All results and summary have been written to {report_dir}")
