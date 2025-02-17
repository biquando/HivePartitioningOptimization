# table.py
from pyhive.hive import Cursor
import time


class Table:
    def __init__(
        self,
        name: str,
        columns: list[tuple[str, str]] | None = None,
        partition: list[tuple[str, str]] | None = None,
    ):
        """Define a table.

        Args:
            columns: List of (str, str) tuples. The first string represents the
                column name, and the second represents the type.
            partition: List of (str, str) tuples in the same format as
                `columns`. These define the columns that should be partitioned
                on. This should be disjoint from `columns`.
        """
        self.name = name
        # Convert lists to dictionaries with column name as key and type as value
        self.columns = (
            {} if columns is None else {name: type_ for name, type_ in columns}
        )
        self.partition = (
            {} if partition is None else {name: type_ for name, type_ in partition}
        )
        self.cardinalities = {}  # Dictionary to store column cardinalities

    def compute_cardinality(self, cursor: Cursor):
        """Compute the cardinality (number of unique values) for each column in the table.
        Updates the cardinality dictionary with results.

        Args:
            cursor: The cursor instance obtained from the Hive connection.
        """
        # Get all columns (both regular and partition)
        all_columns = list(self.columns.keys()) + list(self.partition.keys())

        for col_name in all_columns:
            query = f"""
            SELECT COUNT(DISTINCT {col_name}) as cardinality 
            FROM {self.name}
            """
            cursor.execute(query)
            result = cursor.fetchone()
            self.cardinalities[col_name] = result[0] if result else 0

    def create(self, cursor: Cursor):
        """Create the table in Hive.

        Args:
            cursor: The cursor instance obtained from the Hive connection.
        """
        # Build column definitions
        column_defs = []
        for col_name, col_type in self.columns.items():
            column_defs.append(f"{col_name} {col_type}")

        # Build partition definitions
        partition_defs = []
        for part_name, part_type in self.partition.items():
            partition_defs.append(f"{part_name} {part_type}")

        # Construct the CREATE TABLE query
        query = f"CREATE TABLE {self.name} (\n"
        query += ",\n".join(f"    {col_def}" for col_def in column_defs)
        query += "\n)"

        if partition_defs:
            query += "\nPARTITIONED BY (\n"
            query += ",\n".join(f"    {part_def}" for part_def in partition_defs)
            query += "\n)"

        query += """
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','"""

        cursor.execute(f"DROP TABLE IF EXISTS {self.name}")
        cursor.execute(query)

    def repartition(self, cursor: Cursor, partition_columns: list[str]):
        """Repartition a table by creating a new partitioned table and transferring the data.

        Args:
            cursor: The cursor instance obtained from the Hive connection.
            partition_columns: List of column names to partition the table by.
            These must be existing columns in the table.
        """
        # Get all current column names from both regular and partition columns
        all_current_columns = list(self.columns.keys()) + list(self.partition.keys())

        # Validate partition columns exist in current table
        for col in partition_columns:
            if col not in all_current_columns:
                raise ValueError(f"Column {col} not found in table {self.name}")

        # Create new column and partition dictionaries
        new_columns = {}
        new_partition = {}

        # Sort columns into regular and partition columns
        # Check both current columns and current partition columns
        for col_name in all_current_columns:
            if col_name in partition_columns:
                # Get type from whichever dictionary contains the column
                col_type = self.columns.get(col_name) or self.partition.get(col_name)
                new_partition[col_name] = col_type
            else:
                col_type = self.columns.get(col_name) or self.partition.get(col_name)
                new_columns[col_name] = col_type

        # Create temporary table name
        temp_table_name = f"{self.name}_temp_{int(time.time())}"

        # Create temporary table with new partitioning
        temp_table = Table(
            name=temp_table_name,
            columns=list(new_columns.items()),
            partition=list(new_partition.items()),
        )
        temp_table.create(cursor)

        # Insert data from old table into new table
        # Note: We need to select columns in the correct order
        all_columns = list(new_columns.keys()) + list(new_partition.keys())
        select_cols = ", ".join(all_columns)

        insert_query = f"""
        INSERT OVERWRITE TABLE {temp_table_name}
        PARTITION ({', '.join(partition_columns)})
        SELECT {select_cols} FROM {self.name}
        """
        cursor.execute(insert_query)

        # Drop old table and rename new table
        cursor.execute(f"DROP TABLE {self.name}")
        cursor.execute(f"ALTER TABLE {temp_table_name} RENAME TO {self.name}")

        # Update the object's state to reflect new partitioning
        self.columns = new_columns
        self.partition = new_partition
