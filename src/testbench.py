import fake_data
from table import Table
from pyhive import hive
from query_runner import QueryRunner
from partition_manager import PartitionManager
import argparse
from report_generator import write_consolidated_report
from datetime import datetime

import os
import json
import time


class Testbench:
    def __init__(self, data_size_MiB=2):
        self.conn = hive.Connection(host="localhost", port=10000)
        self.cursor = self.conn.cursor()
        self.data_size_MiB = data_size_MiB

        # Correct memory settings
        # self.cursor.execute("SET mapreduce.map.memory.mb=3072")
        # self.cursor.execute("SET mapreduce.reduce.memory.mb=3072")
        # self.cursor.execute("SET hive.map.aggr.hash.percentmemory=0.5")
        # self.cursor.execute("SET hive.exec.reducers.max=1000")
        # self.cursor.execute("SET hive.vectorized.execution.enabled=true")

        # Parallel execution
        self.cursor.execute("SET hive.exec.parallel=true")
        self.cursor.execute("SET hive.exec.parallel.thread.number=8")

        self.MAX_PARTITION_PRODUCT = 1000
        self.cursor.execute(
            f"SET hive.exec.max.dynamic.partitions={self.MAX_PARTITION_PRODUCT + 5}"
        )
        self.cursor.execute(
            f"SET hive.exec.max.dynamic.partitions.pernode={self.MAX_PARTITION_PRODUCT + 5}"
        )

        # Load column frequency dictionary
        self.column_freq_dict = {}
        with open(
            os.path.join(os.getcwd(), "src", "column_freq_dict.json"), "r"
        ) as file:
            self.column_freq_dict = json.load(file)

        # Load table schemas
        with open(os.path.join(os.getcwd(), "src", "schema.json"), "r") as file:
            schemas = json.load(file)

        # Create tables from schema if they don't exist
        self.tables: dict[str, Table] = {}
        for table_name, schema in schemas.items():
            self.tables[table_name] = Table(table_name, schema)

            # Check if table exists and drop it if it does
            try:
                self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                print(f"Table {table_name} dropped if it existed.")
            except Exception as e:
                print(f"Error when trying to drop table {table_name}: {e}")

            # Create the new table
            print(f"Creating table {table_name}.")
            self.tables[table_name].create(self.cursor)

        # Set up data directory for this size
        self.base_data_dir = os.path.join(os.getcwd(), "data")
        self.size_data_dir = os.path.join(self.base_data_dir, str(data_size_MiB))
        os.makedirs(self.size_data_dir, exist_ok=True)

        # Track current loaded data with a marker file
        self.current_data_marker = os.path.join(
            self.base_data_dir, "current_loaded.txt"
        )

        # Check if we need to generate new data
        should_generate = (
            not os.path.exists(self.size_data_dir)
            or len(os.listdir(self.size_data_dir)) == 0
        )

        # Check if this size data is already loaded in Hive
        current_loaded_size = None
        if os.path.exists(self.current_data_marker):
            with open(self.current_data_marker, "r") as file:
                current_loaded_size = file.read().strip()

        needs_loading = str(data_size_MiB) != current_loaded_size

        # Generate data if needed
        if should_generate:
            print(
                f"Generating {data_size_MiB} MiB of fake data in {self.size_data_dir}"
            )
            fake_data.generate_data(data_size_MiB, output_dir=self.size_data_dir)

            # Since we generated new data, we need to load it
            needs_loading = True

        # Check if tables are empty (additional check)
        if not needs_loading:
            try:
                self.cursor.execute("SELECT COUNT(*) FROM users")
                result = self.cursor.fetchone()
                if result[0] == 0:
                    print("Tables appear to be empty. Will load data.")
                    needs_loading = True
                else:
                    print(f"Table users contains {result[0]} rows.")
            except Exception as e:
                print(f"Error checking table data: {e}")
                needs_loading = True

        # Load data into tables if needed
        if needs_loading:
            # Setup for data loading
            for table_name in self.tables:
                source_path = os.path.join(self.size_data_dir, f"{table_name}.csv")

                if not os.path.exists(source_path):
                    print(f"Warning: CSV file not found: {source_path}")
                    continue

                print(f"Loading {table_name} from size {data_size_MiB} dataset...")

                try:
                    # Try loading directly from the size directory
                    self.cursor.execute(
                        f"LOAD DATA LOCAL INPATH 'file:///data/{data_size_MiB}/{table_name}.csv' OVERWRITE INTO TABLE {table_name}"
                    )
                    print(f"Successfully loaded {table_name} from size directory.")
                except Exception as e:
                    print(f"Error loading {table_name}: {e}")
                    print(f"Could not load {table_name}. This table may be empty.")

            # Update current loaded data marker
            with open(self.current_data_marker, "w") as file:
                file.write(str(data_size_MiB))
            print(f"Data loading operations complete.")
        else:
            print(
                f"Data size {data_size_MiB} MiB is already loaded in Hive, skipping loading step."
            )

        # Compute cardinalities
        for table in self.tables.values():
            table.compute_cardinality(self.cursor)

        # Initialize utility objects
        self.query_runner = QueryRunner(self.cursor)
        self.partition_manager = PartitionManager(
            self.tables, self.cursor, self.column_freq_dict, self.MAX_PARTITION_PRODUCT
        )

    def run(self):
        """Run all queries and return execution time."""
        return self.query_runner.run()

    def algorithm1(self, table_name):
        """Run algorithm 1 for the given table."""
        return self.partition_manager.algorithm1(table_name, self.query_runner)

    def algorithm2(self, table_name):
        """Run algorithm 2 for the given table."""
        return self.partition_manager.algorithm2(table_name, self.query_runner)


def main():
    parser = argparse.ArgumentParser(
        description="Run partitioning algorithms on tables."
    )
    parser.add_argument(
        "--data_size", type=int, default=2, help="Size of the dataset in MiB"
    )
    parser.add_argument(
        "--algorithm",
        type=int,
        choices=[1, 2],
        default=1,
        help="Algorithm to run (1 or 2)",
    )
    parser.add_argument(
        "--tables",
        type=str,
        help="Comma-separated list of tables to run the algorithm on (e.g., 'users,orders'). If not specified, runs on all tables.",
        default=None,
    )

    args = parser.parse_args()

    # Record start time for total execution
    start_time = time.time()

    tb = Testbench(data_size_MiB=args.data_size)

    # Run queries before repartitioning
    # exec_time_1 = tb.run()
    # print(f"Initial query execution time: {round(exec_time_1, 4)}s")

    # Dictionary to store results for all tables
    all_results = {}

    # Determine which tables to process
    if args.tables:
        requested_tables = [t.strip() for t in args.tables.split(",")]
        invalid_tables = [t for t in requested_tables if t not in tb.tables]
        if invalid_tables:
            print(f"Error: Table(s) not found: {invalid_tables}")
            print(f"Available tables: {list(tb.tables.keys())}")
            return
        tables_to_process = requested_tables
    else:
        tables_to_process = list(tb.tables.keys())

    # Iterate over selected tables and run the chosen algorithm
    for table_name in tables_to_process:
        print(f"Running Algorithm {args.algorithm} on table: {table_name}")

        if args.algorithm == 1:
            results = tb.algorithm1(table_name)
        elif args.algorithm == 2:
            results = tb.algorithm2(table_name)
        else:
            print(f"Error: Algorithm {args.algorithm} is not supported.")
            return

        # Store the results
        all_results[table_name] = results

    # Calculate total execution time
    total_time = time.time() - start_time

    # Create metadata dictionary
    metadata = {
        "data_size": args.data_size,
        "total_time": total_time,
        # "initial_all_query_time": exec_time_1,
        "algorithm_version": args.algorithm,
        "num_tables_processed": len(tables_to_process),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_tables_available": len(tb.tables),
    }

    write_consolidated_report(
        all_results, algorithm_name=f"algorithm_{args.algorithm}", metadata=metadata
    )


if __name__ == "__main__":
    main()
