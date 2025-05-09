from __future__ import annotations
import datetime
from typing import TYPE_CHECKING, Any, Callable

from shared import EnumLikeContainer

from .sqlbase import SqlBase

if TYPE_CHECKING:
    from .sqldatabase import SqlDatabase


class SqlDataType(SqlBase):
    database: SqlDatabase

    def __init__(
        self,
        name: str,
        type_: type,
        to_database_converter: Callable[[Any], Any] | None = None,
        from_database_converter: Callable[[Any], Any] | None = None,
    ) -> None:
        self.name = name
        self.type = type_
        self.to_database_converter = to_database_converter
        self.from_database_converter = from_database_converter

    def to_sql(self) -> str:
        return self.name


class SqlIntegerDataType(SqlDataType):
    def __init__(self) -> None:
        SqlDataType.__init__(self, "INTEGER", int)


class SqlFloatDataType(SqlDataType):
    def __init__(self) -> None:
        SqlDataType.__init__(self, "REAL", float)


class SqlTextDataType(SqlDataType):
    def __init__(self) -> None:
        SqlDataType.__init__(self, "TEXT", str)


class SqlBlobDataType(SqlDataType):
    def __init__(self) -> None:
        SqlDataType.__init__(self, "BLOB", bytes)


class SqlBooleanDataType(SqlDataType):
    def __init__(self) -> None:
        SqlDataType.__init__(
            self, "BOOLEAN", bool, self._to_database_value, self._from_database_value
        )

    def _to_database_value(self, value: bool) -> bool | int:
        from .sqlitedatabase import SqliteDatabase

        return int(value) if isinstance(self.database, SqliteDatabase) else value

    def _from_database_value(self, value: bool | int) -> bool:
        return bool(value)

    def to_sql(self) -> str:
        from .sqlitedatabase import SqliteDatabase

        return "INTEGER" if isinstance(self.database, SqliteDatabase) else "BOOLEAN"


class SqlDateDataType(SqlDataType):
    def __init__(self) -> None:
        SqlDataType.__init__(
            self,
            "DATE",
            datetime.date,
            self._to_database_value,
            self._from_database_value,
        )

    def _to_database_value(self, value: datetime.date) -> datetime.date | str:
        from .sqlitedatabase import SqliteDatabase

        return value.isoformat() if isinstance(self.database, SqliteDatabase) else value

    def _from_database_value(self, value: datetime.date | str) -> datetime.date:
        return datetime.date.fromisoformat(value) if isinstance(value, str) else value

    def to_sql(self) -> str:
        from .sqlitedatabase import SqliteDatabase

        return "TEXT" if isinstance(self.database, SqliteDatabase) else "DATE"


class SqlTimeDataType(SqlDataType):
    def __init__(self) -> None:
        SqlDataType.__init__(
            self,
            "TIME",
            datetime.date,
            self._to_database_value,
            self._from_database_value,
        )

    def _to_database_value(self, value: datetime.time) -> datetime.time | str:
        from .sqlitedatabase import SqliteDatabase

        return value.isoformat() if isinstance(self.database, SqliteDatabase) else value

    def _from_database_value(self, value: datetime.time | str) -> datetime.time:
        return datetime.time.fromisoformat(value) if isinstance(value, str) else value

    def to_sql(self) -> str:
        from .sqlitedatabase import SqliteDatabase

        return "TEXT" if isinstance(self.database, SqliteDatabase) else "TIME"


class SqlDateTimeDataType(SqlDataType):
    def __init__(self) -> None:
        SqlDataType.__init__(
            self,
            "DATETIME",
            datetime.date,
            self._to_database_value,
            self._from_database_value,
        )

    def _to_database_value(self, value: datetime.datetime) -> datetime.datetime | str:
        from .sqlitedatabase import SqliteDatabase

        return value.isoformat() if isinstance(self.database, SqliteDatabase) else value

    def _from_database_value(self, value: datetime.datetime | str) -> datetime.datetime:
        return (
            datetime.datetime.fromisoformat(value) if isinstance(value, str) else value
        )

    def to_sql(self) -> str:
        from .sqlitedatabase import SqliteDatabase

        return "TEXT" if isinstance(self.database, SqliteDatabase) else "DATETIME"


class SqlDataTypes(EnumLikeContainer[SqlDataType]):
    item_type = SqlDataType

    BLOB = SqlBlobDataType()
    BOOLEAN = SqlBooleanDataType()
    DATE = SqlDateDataType()
    DATETIME = SqlDateTimeDataType()
    FLOAT = SqlFloatDataType()
    INTEGER = SqlIntegerDataType()
    TEXT = SqlTextDataType()
    TIME = SqlTimeDataType()
