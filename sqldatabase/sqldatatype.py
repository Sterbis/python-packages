import datetime
import sqlite3
from typing import Any, Callable

from shared import EnumLikeContainer

from .sqlbase import SqlBase


class SqlDataType(SqlBase):
    def __init__(
        self,
        name: str,
        type_: type,
        adapter: Callable[[Any], Any] | None = None,
        converter: Callable[[Any], Any] | None = None,
    ) -> None:
        self.name = name
        self.type = type_
        self.adapter = adapter
        self.converter = converter

    def is_native_type(self) -> bool:
        return self.type in (bytes, int, type(None), float, str)

    def to_sql(self) -> str:
        return self.name


class SqlDataTypes(EnumLikeContainer[SqlDataType]):
    item_type = SqlDataType

    BLOB = SqlDataType("BLOB", bytes)
    BOOLEAN = SqlDataType(
        "BOOLEAN", bool, lambda value: 1 if value else 0, lambda value: bool(int(value))
    )
    DATE = SqlDataType(
        "DATE",
        datetime.date,
        lambda value: value.isoformat(),
        lambda value: datetime.date.fromisoformat(value.decode()),
    )
    DATETIME = SqlDataType(
        "DATETIME",
        datetime.datetime,
        lambda value: value.isoformat(),
        lambda value: datetime.datetime.fromisoformat(value.decode()),
    )
    INTEGER = SqlDataType("INTEGER", int)
    NULL = SqlDataType("NULL", type(None))
    REAL = SqlDataType("REAL", float)
    TEXT = SqlDataType("TEXT", str)
    TIMESTAMP = SqlDataType(
        "TIMESTAMP",
        datetime.datetime,
        lambda value: value.timestamp(),
        lambda value: datetime.datetime.fromtimestamp(float(value)),
    )

    def __init__(self) -> None:
        EnumLikeContainer.__init__(self)
        for data_type in self:
            if data_type.adapter is not None:
                sqlite3.register_adapter(data_type.type, data_type.adapter)
            if data_type.converter is not None:
                sqlite3.register_converter(data_type.name, data_type.converter)
