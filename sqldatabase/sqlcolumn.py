from __future__ import annotations
import copy
import uuid
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable

from shared import EnumLikeContainer

from .sqlbase import SqlBase, value_to_sql
from .sqlcolumnfilter import SqlColumnFilters
from .sqldatatype import SqlDataType, SqlDataTypes

if TYPE_CHECKING:
    from .sqltable import SqlTable


class SqlColumn(SqlBase):
    table: SqlTable

    def __init__(
        self,
        name: str,
        data_type: SqlDataType,
        primary_key: bool = False,
        autoincrement: bool = False,
        not_null: bool = False,
        unique: bool = False,
        default: Any = None,
        reference: SqlColumn | None = None,
        adapter: Callable[[Any], Any] | None = None,
        converter: Callable[[Any], Any] | None = None,
        values: type[Enum] | None = None,
    ):
        self.name = name
        self.data_type = data_type
        self.primary_key = primary_key
        self.autoincrement = autoincrement
        self.not_null = not_null
        self.unique = unique
        self.default = default
        self.reference = reference
        self.values = values
        if self.values is None:
            self.adapter = adapter
            self.converter = converter
        else:
            assert (
                adapter is None and converter is None
            ), "Adapter and converter cannot be specified together with values."
            self.adapter = lambda value: value.value
            self.converter = self.values
        self.filters = SqlColumnFilters(self)

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, SqlColumn)
            and self.name == other.name
            and self.table == other.table
        )

    def __deepcopy__(self, memo):
        if id(self) in memo:
            return memo[id(self)]

        cls = self.__class__
        column = cls.__new__(cls)
        memo[id(self)] = column
        for name, value in self.__dict__.items():
            if name not in ("filters", "reference", "table"):
                setattr(column, name, copy.deepcopy(value, memo))

        column.reference = self.reference
        column.filters = SqlColumnFilters(column)
        return column

    def __hash__(self):
        return hash(self.name) + hash(self.table)

    @property
    def alias(self) -> str:
        return f"COLUMN.{self.table.name}.{self.name}"

    @property
    def parameter_name(self) -> str:
        return f"{self.fully_qualified_name.replace('.', '_')}_{uuid.uuid4().hex[:8]}"

    @property
    def fully_qualified_name(self) -> str:
        return f"{self.table.fully_qualified_name}.{self.name}"

    def to_sql(self) -> str:
        return self.name

    def default_to_sql(self) -> str:
        value = self.default
        if self.adapter is not None:
            value = self.adapter(value)
        if self.data_type.adapter is not None:
            value = self.data_type.adapter(value)
        return value_to_sql(value)


class SqlColumns(EnumLikeContainer[SqlColumn]):
    item_type = SqlColumn


class SqlColumnsWithID(SqlColumns):
    ID = SqlColumn("id", SqlDataTypes.INTEGER, primary_key=True, autoincrement=True)
