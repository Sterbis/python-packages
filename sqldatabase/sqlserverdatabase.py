from typing import Generic

import pyodbc  # type: ignore

from .sqldatabase import SqlDatabase, T
from .sqldatatype import SqlDataTypeWithParameter, SqlDataTypes
from .sqltable import SqlTable
from .sqltranspiler import ESqlDialect


class SqlVarcharDataType(SqlDataTypeWithParameter):
    def __init__(self, length: int | str) -> None:
        SqlDataTypeWithParameter.__init__(self, "VARCHAR", str, length)


class SqlNVarcharDataType(SqlDataTypeWithParameter):
    def __init__(self, length: int | str) -> None:
        SqlDataTypeWithParameter.__init__(self, "NVARCHAR", str, length)


class SqlServerDataTypes(SqlDataTypes):
    NVARCHAR = SqlNVarcharDataType
    VARCHAR = SqlVarcharDataType


class SqlServerDatabase(SqlDatabase[T], Generic[T]):
    """
    Represents a SQL Server database.

    Attributes:
        data_types (SqlServerDataTypes): The data types supported by SQL Server.
        dialect (ESqlDialect): The SQL dialect for SQL Server.
        default_schema_name (str): The default schema name (e.g., "dbo").
    """
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
        """
        Initialize a SqlServerDatabase instance.

        Args:
            server (str): The server address.
            database (str): The database name.
            driver (str, optional): The ODBC driver. Defaults to "ODBC Driver 17 for SQL Server".
            trusted_connection (bool, optional): Whether to use a trusted connection. Defaults to True.
            user_id (str | None, optional): The user ID for authentication. Defaults to None.
            password (str | None, optional): The password for authentication. Defaults to None.
            autocommit (bool, optional): Whether to enable autocommit. Defaults to False.
        """
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
