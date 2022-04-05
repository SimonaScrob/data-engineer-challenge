import sqlite3


class SQLiteClient:
    """Context manager for database operations"""
    def __init__(self, db_name):
        self.db_name = db_name
        self.connection = None

    def __enter__(self):
        # creates the connection
        self.connection = sqlite3.connect(self.db_name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    def __execute_query(self, query, params=None):
        if params:
            self.connection.cursor().execute(query, params)
        else:
            self.connection.cursor().execute(query)
        self.connection.commit()

    def create_table(self, table_name, **kwargs):
        """Build the query for creating the table
        :param table_name: string
        :param kwargs: dict (Format: {<column_name>: <column_type>})
        """
        column_name_types = ""
        for column_name, column_type in kwargs.items():
            column_name_types += f"{column_name} {column_type}, "

        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_name_types[:-2]})"

        self.__execute_query(query)

    def insert_row(self, table_name, **kwargs):
        """Build the query for inserting a row
            :param table_name: string
            :param kwargs: dict (Format: {<column_name>: <column_value>})
        """
        params = tuple(list(kwargs.values()))
        column_names = ", ".join(list(kwargs.keys()))
        values = "?, " * len(params)
        query = f"INSERT INTO {table_name} ({column_names}) VALUES ({values[:-2]})"

        self.__execute_query(query, params)
