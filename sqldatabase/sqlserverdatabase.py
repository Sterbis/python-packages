from typing import Generic

import pyodbc  # type: ignore

from .sqldatabase import SqlDatabase, T
from .sqldatatype import SqlDataTypes
from .sqltable import SqlTable
from .sqltranspiler import ESqlDialect


class SqlServerDataTypes(SqlDataTypes):
    pass


class SqlServerDatabase(SqlDatabase[T], Generic[T]):
    data_types = SqlServerDataTypes()
    dialect = ESqlDialect.SQLSERVER
    default_schema_name = "dbo"

    def __init__(
        self,
        server: str,
        database: str,
        driver: str = "ODBC Driver 17 for SQL Server",
        trusted_connection: bool = True,
        user_id: str | None = None,
        password: str | None = None,
        autocommit: bool = False,
    ):
        self.server = server
        self.database = database
        self.driver = driver
        self.trusted_connection = trusted_connection
        self.user_id = user_id
        self.password = password
        self.connection_string = (
            f"Driver={{{self.driver}}};"
            f"Server={self.server};"
            f"Database={self.database};"
        )
        if self.trusted_connection:
            self.connection_string += f"Trusted_Connection={'Yes'};"
        if self.user_id:
            self.connection_string += f"UID={self.user_id};"
        if self.user_id:
            self.connection_string += f"PWD={self.password};"

        connection = pyodbc.connect(self.connection_string, autocommit=autocommit)
        SqlDatabase.__init__(self, database, connection)

    def _parse_table_fully_qualified_name(
        self, table_fully_qualified_name: str
    ) -> tuple[str | None, str | None, str | None]:
        """
        Parse a fully qualified table name into its components.

        Args:
            table_fully_qualified_name (str): The fully qualified table name.

        Returns:
            tuple[str | None, str | None, str | None]: The database name, schema name, and table name.
        """
        table_fully_qualified_name_parts = table_fully_qualified_name.split(".")
        assert (
            len(table_fully_qualified_name_parts) == 3
        ), f"Unexpected table fully qualified name: {table_fully_qualified_name}"
        database_name, schema_name, table_name = table_fully_qualified_name_parts
        return database_name, schema_name, table_name

    def get_table_fully_qualified_name(self, table: SqlTable) -> str:
        return f"{self.name}.{table.schema_name}.{table.name}"
