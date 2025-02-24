import json
import os
from tqdm import tqdm
import time


class QueryRunner:
    def __init__(self, cursor):
        self.cursor = cursor
        self.queries_dir = os.path.join(os.getcwd(), "src", "queries")
        self.table_queries = self._load_all_queries()

    def _load_all_queries(self) -> dict:
        """
        Load all query files into a dictionary during initialization.

        Returns:
            dict: Dictionary mapping table names to their queries
        """
        table_queries = {}
        json_files = [f for f in os.listdir(self.queries_dir) if f.endswith(".json")]

        for json_file in json_files:
            table_name = os.path.splitext(json_file)[0]
            file_path = os.path.join(self.queries_dir, json_file)

            try:
                with open(file_path, "r") as f:
                    table_queries[table_name] = json.load(f)
            except json.JSONDecodeError:
                print(f"Warning: Invalid JSON format in {file_path}")

        return table_queries

    def get_available_tables(self) -> list:
        """Return a list of available table names."""
        return [table for table in self.table_queries.keys() if table != "all"]

    def run(self, table_name: str = None) -> float:
        """
        Run queries for the specified table and return the total execution time.
        If no table_name is provided, runs queries from all.json if it exists,
        otherwise runs all queries from all table files.

        Args:
            table_name (str, optional): Name of the table to run queries for.
                                      If None, runs all queries

        Returns:
            float: Total execution time in seconds

        Raises:
            ValueError: If table_name is invalid
        """
        queries = []
        if table_name is None:
            # If 'all.json' exists, use it
            if "all" in self.table_queries:
                queries = self.table_queries["all"]
            # Otherwise, run all queries from all tables
            else:
                for table_queries in self.table_queries.values():
                    queries.extend(table_queries)
            print("Running all queries...")
        else:
            if table_name not in self.table_queries:
                available_tables = self.get_available_tables()
                raise ValueError(
                    f"Invalid table name: {table_name}. "
                    f"Available tables: {', '.join(available_tables)}"
                )
            queries = self.table_queries[table_name]
            print(f"Running queries for table: {table_name}...")

        start = time.time()
        for query in tqdm(queries):
            self.cursor.execute(query)
        end = time.time()

        return end - start
