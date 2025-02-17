import os


def write_report(results, table_name, algorithm_name):
    """Writes the results to a report file in the specified directory."""
    # Ensure the directory exists
    report_dir = f"algorithm_reports/{algorithm_name.lower()}"
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)

    # Define the filename based on the table name
    report_filename = f"{report_dir}/{table_name}_report.txt"

    # Write the results for this table to the file
    with open(report_filename, "w") as file:
        file.write(
            f"{algorithm_name} results for table {table_name} (sorted by execution time):\n"
        )
        file.write(
            f"{'Partition Columns':<30} {'Execution Time (s)':<20} {'Cardinality Product':<25} {'Time Difference (%)'}\n"
        )
        file.write(f"{'-' * 80}\n")

        # Find the baseline (no partitioning) execution time
        no_partition_time = next(
            (time for columns, time, _ in results if columns == []), None
        )

        for columns, time, cardinality_product in results:
            if no_partition_time is not None and time != float("inf"):
                # Calculate percentage time difference from no partitioning
                time_diff_percent = (
                    (time - no_partition_time) / no_partition_time
                ) * 100
            else:
                time_diff_percent = None  # If no time difference can be calculated (e.g., exceeds limit)

            file.write(
                f"{str(columns):<30} {round(time, 4):<20} {cardinality_product:<25} {time_diff_percent if time_diff_percent is None else round(time_diff_percent, 2)}\n"
            )

    print(f"Results for table {table_name} have been written to {report_filename}")
