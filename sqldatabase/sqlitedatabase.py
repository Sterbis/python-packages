import sqlite3
from pathlib import Path
from typing import Any, Generic

from .sqldatabase import SqlDatabase, T
from .sqldatatype import SqlDataTypes
from .sqltranspiler import ESqlDialect


class SqliteDataTypes(SqlDataTypes):
    pass


class SqliteDatabase(SqlDatabase[T], Generic[T]):
    data_types = SqliteDataTypes()
    dialect = ESqlDialect.SQLITE

    def __init__(self, path: str | Path, autocommit: bool = False):
        self.path = Path(path)
        connection = sqlite3.connect(self.path, autocommit=autocommit)
        SqlDatabase.__init__(self, "main", connection)
