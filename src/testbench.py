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

        # Correct memory settings
        self.cursor.execute("SET mapreduce.map.memory.mb=3072")
        self.cursor.execute("SET mapreduce.reduce.memory.mb=3072")
        self.cursor.execute("SET hive.map.aggr.hash.percentmemory=0.5")
        self.cursor.execute("SET hive.exec.reducers.max=1000")
        self.cursor.execute("SET hive.vectorized.execution.enabled=true")

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

        # Create tables from schema
        self.tables: dict[str, Table] = {}
        for table_name, schema in schemas.items():
            self.tables[table_name] = Table(table_name, schema)
            self.cursor.execute(f"DROP TABLE {table_name}")
            self.tables[table_name].create(self.cursor)

        # Check if we need to generate new data
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

        # Load data into tables
        for table_name in self.tables:
            print(f"Loading {table_name}...")
            self.cursor.execute(
                f"LOAD DATA LOCAL INPATH 'file:///data/{table_name}.csv' OVERWRITE INTO TABLE {table_name}"
            )

        # Compute cardinalities
        for table in self.tables.values():
            table.compute_cardinality(self.cursor)

        # Initialize utility objects
        self.query_runner = QueryRunner(self.cursor)
        self.partition_manager = PartitionManager(
            self.tables, self.cursor, self.column_freq_dict, self.MAX_PARTITION_PRODUCT
        )

        print("Data successfully loaded into tables.")

    def run(self):
        """Run all queries and return execution time."""
        return self.query_runner.run()

    def algorithm1(self, table_name):
        """Run algorithm 1 for the given table."""
        return self.partition_manager.algorithm1(table_name, self.query_runner)

    def algorithm2(self, table_name):
        """Run algorithm 2 for the given table."""
        return self.partition_manager.algorithm1(table_name, self.query_runner)


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
