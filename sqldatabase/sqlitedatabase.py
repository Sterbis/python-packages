import sqlite3
from pathlib import Path
from typing import Generic

from .sqldatabase import SqlDatabase, T
from .sqldatatype import SqlDataTypes
from .sqltable import SqlTable
from .sqltranspiler import ESqlDialect


class SqliteDataTypes(SqlDataTypes):
    """Represents SQLite-specific data types."""


class SqliteDatabase(SqlDatabase[T], Generic[T]):
    """Represents a SQLite database.

    Attributes:
        dialect (ESqlDialect): The SQL dialect used by the database.
        path (Path): The file path to the SQLite database.
    """

    dialect = ESqlDialect.SQLITE

    def __init__(self, path: str | Path, autocommit: bool = False):
        """Initialize a SqliteDatabase instance.

        Args:
            path (str | Path): The file path to the SQLite database.
            autocommit (bool, optional): Whether to enable autocommit mode. Defaults to False.
        """
        self.path = Path(path)
        connection = sqlite3.connect(self.path, autocommit=autocommit)
        SqlDatabase.__init__(self, "main", connection)

    def _parse_table_fully_qualified_name(
        self,
        table_fully_qualified_name: str,
    ) -> tuple[str | None, str | None, str | None]:
        """Parse a fully qualified table name into its components.

        Args:
            table_fully_qualified_name (str): The fully qualified table name.

        Returns:
            tuple[str | None, str | None, str | None]: A tuple containing the database name, schema name, and table name.
        """
        database_name = schema_name = table_name = None
        table_fully_qualified_name_parts = table_fully_qualified_name.split(".")
        if len(table_fully_qualified_name_parts) == 1:
            table_name = table_fully_qualified_name_parts[0]
        elif len(table_fully_qualified_name_parts) == 2:
            database_name, table_name = table_fully_qualified_name_parts
        else:
            assert (
                False
            ), f"Unexpected table fully qualified name: {table_fully_qualified_name}"

        return database_name, schema_name, table_name

    def get_table_fully_qualified_name(self, table: SqlTable) -> str:
        """Get the fully qualified name of a table.

        Args:
            table (SqlTable): The table for which to get the fully qualified name.

        Returns:
            str: The fully qualified name of the table.
        """
        if self.attached_databases:
            return f"{self.name}.{table.name}"
        else:
            return table.name
