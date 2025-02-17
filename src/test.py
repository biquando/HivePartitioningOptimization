import fake_data
from queries import queries
from table import Table

from pyhive import hive
from tqdm import tqdm

import os
import time
import json

from report_generator import write_report


class Testbench:
    def __init__(self, data_size_MiB=10):
        self.conn = hive.Connection(host="localhost", port=10000)
        self.cursor = self.conn.cursor()
        self.MAX_PARTITION_PRODUCT = 1000
        self.cursor.execute(
            f"SET hive.exec.max.dynamic.partitions={self.MAX_PARTITION_PRODUCT + 1}"
        )
        self.cursor.execute(
            f"SET hive.exec.max.dynamic.partitions.pernode={self.MAX_PARTITION_PRODUCT + 1}"
        )

        self.column_freq_dict = {}
        with open(
            os.path.join(os.getcwd(), "src", "column_freq_dict.json"), "r"
        ) as file:
            self.column_freq_dict = json.load(file)

        # Create tables
        self.tables: dict[str, Table] = {}
        self.tables["users"] = Table(
            "users",
            [
                ("user_id", "INT"),
                ("name", "STRING"),
                ("email", "STRING"),
                ("created_at", "TIMESTAMP"),
            ],
        )
        self.tables["products"] = Table(
            "products",
            [
                ("product_id", "INT"),
                ("name", "STRING"),
                ("category", "STRING"),
                ("price", "DECIMAL(10,2)"),
                ("stock", "INT"),
            ],
        )
        self.tables["orders"] = Table(
            "orders",
            [
                ("order_id", "INT"),
                ("user_id", "INT"),
                ("order_date", "TIMESTAMP"),
                ("total_amount", "DECIMAL(10,2)"),
            ],
        )
        self.tables["order_items"] = Table(
            "order_items",
            [
                ("order_id", "INT"),
                ("product_id", "INT"),
                ("quantity", "INT"),
                ("price", "DECIMAL(10,2)"),
            ],
        )
        self.tables["reviews"] = Table(
            "reviews",
            [
                ("review_id", "INT"),
                ("user_id", "INT"),
                ("product_id", "INT"),
                ("rating", "INT"),
                ("comment", "STRING"),
            ],
        )
        for table_name in self.tables:
            self.cursor.execute(f"DROP TABLE {table_name}")
            self.tables[table_name].create(self.cursor)

        # Populate tables with data (use cached data if possible)
        should_generate = not os.path.exists(
            os.path.join(os.getcwd(), "data", "size.txt")
        )
        if not should_generate:
            with open(os.path.join(os.getcwd(), "data", "size.txt"), "r") as file:
                prev_size = int(file.read())
            if data_size_MiB != prev_size:
                should_generate = True
        if should_generate:
            with open(os.path.join(os.getcwd(), "data", "size.txt"), "w") as file:
                file.write(str(data_size_MiB) + "\n")
            print(f"Generating {data_size_MiB} MiB of fake data.")
            fake_data.generate_data(data_size_MiB)

        print("Loading users...")
        self.cursor.execute(
            "LOAD DATA LOCAL INPATH 'file:///data/users.csv' OVERWRITE INTO TABLE users"
        )
        print("Loading products...")
        self.cursor.execute(
            "LOAD DATA LOCAL INPATH 'file:///data/products.csv' OVERWRITE INTO TABLE products"
        )
        print("Loading orders...")
        self.cursor.execute(
            "LOAD DATA LOCAL INPATH 'file:///data/orders.csv' OVERWRITE INTO TABLE orders"
        )
        print("Loading order_items...")
        self.cursor.execute(
            "LOAD DATA LOCAL INPATH 'file:///data/order_items.csv' OVERWRITE INTO TABLE order_items"
        )
        print("Loading reviews...")
        self.cursor.execute(
            "LOAD DATA LOCAL INPATH 'file:///data/reviews.csv' OVERWRITE INTO TABLE reviews"
        )

        # compute cardinalities
        for table in self.tables.values():
            table.compute_cardinality(self.cursor)

        print("Data successfully loaded into tables.")

    def run(self) -> float:
        print("Running queries...")
        start = time.time()
        for query in tqdm(queries):
            self.cursor.execute(query)
        end = time.time()
        return end - start

    def repartition(self, table_name: str, partition_columns: list[str]) -> float:
        """Repartition a specific table by the given columns.

        Args:
            table_name: Name of the table to repartition
            partition_columns: List of column names to partition by

        Returns:
            float: Time taken to perform the repartitioning in seconds
        """
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found")

        table = self.tables[table_name]
        print(f"Repartitioning {table_name} by {partition_columns}...")

        start = time.time()
        table.repartition(self.cursor, partition_columns)
        end = time.time()

        return end - start

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

    def check_repartition_cardinality(self, repartition_columns, table_name):
        """Check the cardinality of the repartition columns in the table."""
        table = self.tables[table_name]
        product = 1
        cardinalities = []
        for col in repartition_columns:
            cardinality = table.cardinalities[col]
            cardinalities.append((col, cardinality))
            product *= cardinality
        return product <= self.MAX_PARTITION_PRODUCT, product

    def attempt_repartition_and_run(self, table_name, repartition_columns):
        """Attempt to repartition the table and run the queries."""
        valid_partition, cardinality_product = self.check_repartition_cardinality(
            repartition_columns, table_name
        )
        if valid_partition:
            self.repartition(table_name, repartition_columns)
            return self.run(), cardinality_product
        else:
            print(
                f"Repartitioning {table_name} by {repartition_columns} exceeds max partitions."
            )
            return float("inf"), cardinality_product

    def algorithm1(self, table_name: str) -> list[tuple[list[str], float, int]]:
        """Implement Algorithm 1 for partition column selection."""
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found")

        table = self.tables[table_name]
        query_execution_times = []

        # Get frequencies for this table
        table_frequencies = self.column_freq_dict.get(table_name, {})

        # Sort columns by frequency
        sorted_columns = sorted(
            table_frequencies.items(), key=lambda x: x[1], reverse=True
        )

        # Get top 3 most frequently used columns
        top_columns = [col for col, _ in sorted_columns[:3]]

        # Test no partition (baseline case)
        exec_time_no_partition = self.run()  # Run without any repartitioning
        query_execution_times.append(
            ([], exec_time_no_partition, 1)
        )  # No partition, product = 1 (or can be set as 0)

        # Test single column combinations
        for col in top_columns:
            exec_time, cardinality_product = self.attempt_repartition_and_run(
                table_name, [col]
            )
            query_execution_times.append(([col], exec_time, cardinality_product))

        # Test two column combinations
        for i in range(len(top_columns)):
            for j in range(i + 1, len(top_columns)):
                cols = [top_columns[i], top_columns[j]]
                exec_time, cardinality_product = self.attempt_repartition_and_run(
                    table_name, cols
                )
                query_execution_times.append((cols, exec_time, cardinality_product))

        # Test three column combination
        if len(top_columns) >= 3:
            cols = top_columns[:3]
            exec_time, cardinality_product = self.attempt_repartition_and_run(
                table_name, cols
            )
            query_execution_times.append((cols, exec_time, cardinality_product))

        # Sort by execution time
        return sorted(query_execution_times, key=lambda x: x[1])


def main():
    tb = Testbench(data_size_MiB=1)

    # Run queries before repartitioning
    exec_time_1 = tb.run()
    print(f"Initial query execution time: {round(exec_time_1, 4)}s")

    # Iterate over all tables and run Algorithm 1
    for table_name in tb.tables:
        print(f"Running Algorithm 1 on table: {table_name}")

        # Run Algorithm 1 on each table
        results = tb.algorithm1(table_name)

        # Write the results using the abstracted function
        write_report(results, table_name, algorithm_name="algorithm_1")


if __name__ == "__main__":
    main()
