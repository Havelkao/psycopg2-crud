import pandas as pd
from psycopg2.extensions import connection
from psycopg2.sql import SQL, Identifier, Placeholder
from psycopg2.extras import execute_values
from .helpers import handle_db_errors, for_all_methods


@for_all_methods(handle_db_errors)
class CRUD:
    """Base class that adds convenience methods for CRUD operations."""

    def __init__(self, conn: connection, table: str, columns: tuple[str] = None):
        self.table = table
        self.conn = conn
        self.columns = columns
        self.cursor = conn.cursor()

    def select(
        self, columns: tuple = SQL("*"), filter_by: dict = {}, fetch_one: bool = False
    ) -> pd.DataFrame:
        """
        Select columns with where clause, return pandas dataframe.
        """
        column_labels = self.columns
        template = "SELECT {} FROM {}"
        query_params = []

        for index, key in enumerate(filter_by):
            condition_syntax = " WHERE" if index == 0 else " AND"
            template += condition_syntax + " {} = {}"
            query_params.extend([Identifier(key), Placeholder()])

        if type(columns) is tuple:
            column_labels = columns
            columns = SQL(",").join(map(Identifier, columns))

        query = SQL(template).format(columns, Identifier(self.table), *query_params)
        self.cursor.execute(query, tuple(filter_by.values()))

        if fetch_one is True:
            data = self.cursor.fetchone()
        else:
            data = self.cursor.fetchall()

        return pd.DataFrame(data, columns=column_labels)

    def get_id(self, filter_by: dict = {}):
        """
        Get first id of first row that matches where condition.
        """
        return self.select(("id",), filter_by, fetch_one=True).iloc[0]["id"]

    def insert(self, commit=True, **kwargs):
        """
        Insert a single row into the table.
        """
        query = SQL("INSERT INTO {} ({}) VALUES ({})").format(
            Identifier(self.table),
            SQL(",").join(map(Identifier, kwargs.keys())),
            SQL(",").join(Placeholder() * len(kwargs.values())),
        )

        self.cursor.execute(query, tuple(kwargs.values()))

        if commit is True:
            self.conn.commit()

    def insert_df(self, df: pd.DataFrame, commit=True):
        """
        Insert a pandas dataframe into the table.
        """
        query = SQL("INSERT INTO  {} ({}) VALUES {}").format(
            Identifier(self.table),
            SQL(",").join(map(Identifier, df.keys())),
            Placeholder(),
        )

        execute_values(self.cursor, query, df.values)

        if commit is True:
            self.conn.commit()

    def update(self, data: dict, filter_by: dict = {}, commit=True):
        """
        Update column values in specifed records
        """
        template = "UPDATE {} SET " + ", ".join(["{} = {}"] * len(data))
        query_params = []
        values = []

        for key, value in data.items():
            query_params.extend([Identifier(key), Placeholder()])
            values.append(value)

        for index, (key, value) in enumerate(filter_by.items()):
            condition_syntax = " WHERE" if index == 0 else " AND"
            template += condition_syntax + " {} = {}"
            query_params.extend([Identifier(key), Placeholder()])
            values.append(value)

        query = SQL(template).format(Identifier(self.table), *query_params)

        self.cursor.execute(query, values)
        if commit is True:
            self.conn.commit()

    def delete(self, filter_by: dict = {}, commit=True):
        """
        Delete records based on where condition
        """
        template = "DELETE FROM {}"
        query_params = []

        for index, key in enumerate(filter_by):
            condition_syntax = " WHERE" if index == 0 else " AND"
            template += condition_syntax + " {} = {}"
            query_params.extend([Identifier(key), Placeholder()])

        query = SQL(template).format(Identifier(self.table), *query_params)

        self.cursor.execute(query)
        if commit is True:
            self.conn.commit()
