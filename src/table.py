from pyhive.hive import Cursor

class Table:
    def __init__(
            self,
            name: str,
            columns: list[tuple[str, str]] | None = None,
            partition: list[tuple[str, str]] | None = None
    ):
        '''Define a table.

        Args:
            columns: List of (str, str) tuples. The first string represents the
                column name, and the second represents the type.
            partition: List of (str, str) tuples in the same format as
                `columns`. These define the columns that should be partitioned
                on. This should be disjoint from `columns`.
        '''
        self.name = name
        self.columns = [] if columns is None else columns
        self.partition = [] if partition is None else partition

    def create(self, cursor: Cursor):
        '''Create the table in Hive.

        Args:
            cursor: The cursor instance obtained from the Hive connection.
        '''
        query = f'CREATE TABLE {self.name} (\n'
        if len(self.columns) > 0:
            for col in self.columns[:-1]:
                query += f'    {col[0]} {col[1]},\n'
            query += f'    {self.columns[-1][0]} {self.columns[-1][1]}\n'
        if len(self.partition) > 0:
            query += ')\nPARTITIONED BY (\n'
            for part in self.partition:
                query += f'    {part[0]} {part[1]},\n'
            query += f'    {self.partition[-1][0]} {self.partition[-1][1]}\n'
        query += """)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','"""

        cursor.execute(query)
