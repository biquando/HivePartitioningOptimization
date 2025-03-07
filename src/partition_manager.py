# parition_manager.py
from table import Table
import time


class PartitionManager:
    def __init__(self, tables, cursor, column_freq_dict, MAX_PARTITION_PRODUCT=1000):
        self.tables = tables
        self.cursor = cursor
        self.column_freq_dict = column_freq_dict
        self.MAX_PARTITION_PRODUCT = MAX_PARTITION_PRODUCT

    def check_repartition_cardinality(self, repartition_columns, table_name):
        """Check the cardinality of the repartition columns in the table."""
        table = self.tables[table_name]
        product = 1
        cardinalities = []
        for col in repartition_columns:
            cardinality = table.cardinalities[col]
            cardinalities.append((col, cardinality))
            product *= cardinality

        print(
            f"Cardinalities for {table_name} and cols {repartition_columns}: {cardinalities}"
        )
        return product <= self.MAX_PARTITION_PRODUCT, product

    def repartition(self, table_name, partition_columns):
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

    def attempt_repartition_and_run(
        self, table_name, repartition_columns, query_runner
    ):
        """Attempt to repartition the table and run the queries."""
        valid_partition, cardinality_product = self.check_repartition_cardinality(
            repartition_columns, table_name
        )
        if valid_partition:
            self.repartition(table_name, repartition_columns)
            return query_runner.run(table_name), cardinality_product
        else:
            print(
                f"Repartitioning {table_name} by {repartition_columns} exceeds max partitions."
            )
            return float("inf"), cardinality_product

    def algorithm1(self, table_name, query_runner):
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
        exec_time_no_partition = query_runner.run(
            table_name
        )  # Run without any repartitioning
        query_execution_times.append(
            ([], exec_time_no_partition, 1)
        )  # No partition, product = 1 (or can be set as 0)

        # Test single column combinations
        for col in top_columns:
            exec_time, cardinality_product = self.attempt_repartition_and_run(
                table_name, [col], query_runner
            )
            query_execution_times.append(([col], exec_time, cardinality_product))

        # Test two column combinations
        for i in range(len(top_columns)):
            for j in range(i + 1, len(top_columns)):
                cols = [top_columns[i], top_columns[j]]
                exec_time, cardinality_product = self.attempt_repartition_and_run(
                    table_name, cols, query_runner
                )
                query_execution_times.append((cols, exec_time, cardinality_product))

        # Test three column combination
        if len(top_columns) >= 3:
            cols = top_columns[:3]
            exec_time, cardinality_product = self.attempt_repartition_and_run(
                table_name, cols, query_runner
            )
            query_execution_times.append((cols, exec_time, cardinality_product))

        # Sort by execution time
        return sorted(query_execution_times, key=lambda x: x[1])

    def algorithm2(self, table_name, query_runner):
        """Implement Algorithm 2 for partition column selection using a greedy approach."""
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found")

        table = self.tables[table_name]
        query_execution_times = []
        best_columns = []
        best_exec_time = float("inf")

        # Get frequencies for this table
        table_frequencies = self.column_freq_dict.get(table_name, {})

        # Get all columns (without sorting by frequency)
        all_columns = list(table_frequencies.keys())

        # Test no partition (baseline case)
        exec_time_no_partition = query_runner.run(
            table_name
        )  # Run without any repartitioning
        best_exec_time = exec_time_no_partition
        query_execution_times.append(
            ([], exec_time_no_partition, 1)
        )  # No partition, product = 1

        # Iteratively add the best column
        remaining_columns = all_columns.copy()

        while remaining_columns:
            iteration_best_col = None
            iteration_best_time = float("inf")
            iteration_best_product = 0

            # Try adding each remaining column to our best set
            for col in remaining_columns:
                candidate_columns = best_columns + [col]
                exec_time, cardinality_product = self.attempt_repartition_and_run(
                    table_name, candidate_columns, query_runner
                )

                # Record all tested combinations
                query_execution_times.append(
                    (candidate_columns, exec_time, cardinality_product)
                )

                # Track the best for this iteration
                if exec_time < iteration_best_time:
                    iteration_best_col = col
                    iteration_best_time = exec_time
                    iteration_best_product = cardinality_product

            # If adding the best column makes performance worse, stop
            if iteration_best_time >= best_exec_time:
                break

            # Otherwise, update our best configuration
            best_columns.append(iteration_best_col)
            best_exec_time = iteration_best_time

            # Remove the column we just added from the remaining set
            remaining_columns.remove(iteration_best_col)

            # Return all tested combinations, sorted by execution time
        return sorted(query_execution_times, key=lambda x: x[1])

    # def algorithm2(self, table_name, query_runner):
    #     if table_name not in self.tables:
    #         raise ValueError(f"Table {table_name} not found")

    #     remaining_cols = self.column_freq_dict.get(table_name, {})

    #     partition_cols = []

    #     query_execution_times = []

    #     exec_time_no_partition = query_runner.run(
    #         table_name
    #     )  # Run without any repartitioning

    #     query_execution_times.append(
    #         ([], exec_time_no_partition, 1)
    #     )  # No partition, product = 1 (or can be set as 0)

    #     new_exec_time = exec_time_no_partition
    #     old_exec_time = exec_time_no_partition + 1  # Initialize old_exec_time to be necessarily
    #                                                 # bigger than new_exec_time.

    #     while new_exec_time < old_exec_time:
    #         old_exec_time = new_exec_time
    #         best_col = None

    #         for col in remaining_cols:
    #             exec_time, cardinality_product = self.attempt_repartition_and_run(
    #                 table_name, partition_cols + [col], query_runner
    #             )

    #             if exec_time < new_exec_time:
    #                 best_col = col
    #                 new_cardinality_product = cardinality_product
    #                 new_exec_time = exec_time

    #         if best_col is not None:
    #             partition_cols.append(best_col)
    #             query_execution_times.append((partition_cols.copy(), new_exec_time, new_cardinality_product))
    #             remaining_cols.pop(best_col)

    #     print("DEBUG: At partition_mamanger: query_execution_times = ", query_execution_times)
    #     return query_execution_times
