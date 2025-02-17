# query_runner.py
from queries import queries
from tqdm import tqdm
import time


class QueryRunner:
    def __init__(self, cursor):
        self.cursor = cursor
        self.queries = queries

    def run(self) -> float:
        """Run all queries and return the total execution time."""
        print("Running queries...")
        start = time.time()
        for query in tqdm(self.queries):
            self.cursor.execute(query)
        end = time.time()
        return end - start
