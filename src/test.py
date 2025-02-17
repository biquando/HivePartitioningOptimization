import fake_data
from queries import queries
from table import Table

from pyhive import hive
from tqdm import tqdm

import os
import time


class Testbench:
    def __init__(self, data_size_MiB=10):
        self.conn = hive.Connection(host="localhost", port=10000)
        self.cursor = self.conn.cursor()

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


def main():
    tb = Testbench(data_size_MiB=1)

    # Run queries before repartitioning
    exec_time_1 = tb.run()
    print(f"Took {round(exec_time_1, 4)}s to run queries.")

    # Repartition orders table and run queries again
    repart_time = tb.repartition("orders", ["order_date"])
    print(f"Took {round(repart_time, 4)}s to repartition the table.")

    exec_time_2 = tb.run()
    print(f"Took {round(exec_time_2, 4)}s to run queries after repartitioning.")

    # Print improvement
    improvement = ((exec_time_1 - exec_time_2) / exec_time_1) * 100
    print(f"Query performance improved by {round(improvement, 2)}%")


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
