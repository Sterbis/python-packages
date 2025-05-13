import sqlite3
from pathlib import Path
from typing import Generic

from .sqldatabase import SqlDatabase, T
from .sqldatatype import SqlDataTypes
from .sqltable import SqlTable
from .sqltranspiler import ESqlDialect


class SqliteDataTypes(SqlDataTypes):
    pass


class SqliteDatabase(SqlDatabase[T], Generic[T]):
    dialect = ESqlDialect.SQLITE

    def __init__(self, path: str | Path, autocommit: bool = False):
        self.path = Path(path)
        connection = sqlite3.connect(self.path, autocommit=autocommit)
        SqlDatabase.__init__(self, "main", connection)

    def _parse_table_fully_qualified_name(
        self, table_fully_qualified_name: str,
    ) -> tuple[str | None, str | None, str | None]:
        database_name = schema_name = table_name = None
        table_fully_qualified_name_parts = table_fully_qualified_name.split(".")
        if len(table_fully_qualified_name_parts) == 1:
            table_name = table_fully_qualified_name_parts[0]
        elif len(table_fully_qualified_name_parts) == 2:
            database_name, table_name = table_fully_qualified_name_parts
        else:
            assert False, f"Unexpected table fully qualified name: {table_fully_qualified_name}"

        return database_name, schema_name, table_name

    def get_table_fully_qualified_name(self, table: SqlTable) -> str:
        if self.attached_databases:
            return f"{self.name}.{table.name}"
        else:
            return table.name
