from __future__ import annotations
import datetime
from typing import TYPE_CHECKING, Any, Callable

from shared import EnumLikeMixedContainer

from .sqlbase import SqlBase

if TYPE_CHECKING:
    from .sqldatabase import SqlDatabase


class SqlDataType(SqlBase):
    """
    Represents a SQL data type.

    Attributes:
        name (str): The name of the data type.
        type (type): The Python type corresponding to the SQL data type.
        to_database_converter (Callable[[Any], Any] | None): Function to convert values to database format.
        from_database_converter (Callable[[Any], Any] | None): Function to convert values from database format.
    """
    database: SqlDatabase

    def __init__(
        self,
        name: str,
        type_: type,
        to_database_converter: Callable[[Any], Any] | None = None,
        from_database_converter: Callable[[Any], Any] | None = None,
    ) -> None:
        """
        Initialize a SqlDataType instance.

        Args:
            name (str): The name of the data type.
            type_ (type): The Python type corresponding to the SQL data type.
            to_database_converter (Callable[[Any], Any] | None, optional): Function to convert values to database format. Defaults to None.
            from_database_converter (Callable[[Any], Any] | None, optional): Function to convert values from database format. Defaults to None.
        """
        self.name = name
        self.type = type_
        self.to_database_converter = to_database_converter
        self.from_database_converter = from_database_converter

    def to_sql(self) -> str:
        """
        Convert the data type to its SQL representation.

        Returns:
            str: The SQL representation of the data type.
        """
        return self.name


class SqlDataTypeWithParameter(SqlDataType):
    def __init__(
        self,
        name: str,
        type_: type,
        parameter: Any,
        to_database_converter: Callable[[Any], Any] | None = None,
        from_database_converter: Callable[[Any], Any] | None = None,
    ) -> None:
        SqlDataType.__init__(self, name, type_, to_database_converter, from_database_converter)
        self.parameter = parameter

    def to_sql(self) -> str:
        return f"{self.name}({self.parameter})"


class SqlIntegerDataType(SqlDataType):
    def __init__(self) -> None:
        SqlDataType.__init__(self, "INTEGER", int)


class SqlFloatDataType(SqlDataType):
    def __init__(self) -> None:
        SqlDataType.__init__(self, "REAL", float)


class SqlTextDataType(SqlDataType):
    def __init__(self) -> None:
        SqlDataType.__init__(self, "TEXT", str)

    def to_sql(self) -> str:
        from .sqlserverdatabase import SqlServerDatabase

        return "NVARCHAR(255)" if isinstance(self.database, SqlServerDatabase) else self.name


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

        return "INTEGER" if isinstance(self.database, SqliteDatabase) else self.name


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

        return "TEXT" if isinstance(self.database, SqliteDatabase) else self.name


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

        return "TEXT" if isinstance(self.database, SqliteDatabase) else self.name


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

        return "TEXT" if isinstance(self.database, SqliteDatabase) else self.name


class SqlDataTypes(EnumLikeMixedContainer[SqlDataType]):
    item_type = SqlDataType

    BLOB = SqlBlobDataType()
    BOOLEAN = SqlBooleanDataType()
    DATE = SqlDateDataType()
    DATETIME = SqlDateTimeDataType()
    FLOAT = SqlFloatDataType()
    INTEGER = SqlIntegerDataType()
    TEXT = SqlTextDataType()
    TIME = SqlTimeDataType()
