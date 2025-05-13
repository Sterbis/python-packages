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
    """
    Represents a column in a SQL table.

    Attributes:
        name (str): The name of the column.
        data_type (SqlDataType): The data type of the column.
        primary_key (bool): Whether the column is a primary key.
        autoincrement (bool): Whether the column is auto-incremented.
        not_null (bool): Whether the column is NOT NULL.
        unique (bool): Whether the column is unique.
        default (Any): The default value of the column.
        reference (SqlColumn | None): A reference to another column.
        to_database_converter (Callable[[Any], Any] | None): Function to convert values to database format.
        from_database_converter (Callable[[Any], Any] | None): Function to convert values from database format.
        values (type[Enum] | None): Enum values for the column.
    """
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
        to_database_converter: Callable[[Any], Any] | None = None,
        from_database_converter: Callable[[Any], Any] | None = None,
        values: type[Enum] | None = None,
    ):
        """
        Initialize a SqlColumn instance.

        Args:
            name (str): The name of the column.
            data_type (SqlDataType): The data type of the column.
            primary_key (bool, optional): Whether the column is a primary key. Defaults to False.
            autoincrement (bool, optional): Whether the column is auto-incremented. Defaults to False.
            not_null (bool, optional): Whether the column is NOT NULL. Defaults to False.
            unique (bool, optional): Whether the column is unique. Defaults to False.
            default (Any, optional): The default value of the column. Defaults to None.
            reference (SqlColumn | None, optional): A reference to another column. Defaults to None.
            to_database_converter (Callable[[Any], Any] | None, optional): Function to convert values to database format. Defaults to None.
            from_database_converter (Callable[[Any], Any] | None, optional): Function to convert values from database format. Defaults to None.
            values (type[Enum] | None, optional): Enum values for the column. Defaults to None.
        """
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
            self.to_database_converter = to_database_converter
            self.from_database_converter = from_database_converter
        else:
            assert (
                to_database_converter is None and from_database_converter is None
            ), "Converters cannot be specified together with values."
            self.to_database_converter = lambda value: value.value
            self.from_database_converter = self.values
        self.filters = SqlColumnFilters(self)
        self._foreign_keys: list[SqlColumn] = []
        if self.reference is not None:
            self.reference._foreign_keys.append(self)
        self.table: SqlTable | None = None

    def __deepcopy__(self, memo) -> SqlColumn:
        if id(self) in memo:
            return memo[id(self)]

        cls = self.__class__
        column = cls.__new__(cls)
        memo[id(self)] = column
        for name, value in self.__dict__.items():
            if name not in ("_foreign_keys", "filters", "reference", "table"):
                setattr(column, name, copy.deepcopy(value, memo))

        column.filters = SqlColumnFilters(column)
        column.reference = self.reference
        if column.reference is not None:
            column.reference._foreign_keys.remove(self)
            column.reference._foreign_keys.append(column)
        column._foreign_keys = copy.copy(self._foreign_keys)
        for foreign_key in column._foreign_keys:
            foreign_key.reference = column
        return column

    @property
    def alias(self) -> str:
        return f"COLUMN.{self.fully_qualified_name}"

    @property
    def fully_qualified_name(self) -> str:
        if self.table is None:
            raise AttributeError("The 'table' attribute is not set for this column.")
        return f"{self.table.fully_qualified_name}.{self.name}"

    def generate_parameter_name(self) -> str:
        return f"{self.fully_qualified_name.replace('.', '_')}_{uuid.uuid4().hex[:8]}"

    def to_sql(self) -> str:
        return self.name

    def default_to_sql(self) -> str:
        value = self.default
        if self.to_database_converter is not None:
            value = self.to_database_converter(value)
        if self.data_type.to_database_converter is not None:
            value = self.data_type.to_database_converter(value)
        return value_to_sql(value)


class SqlColumns(EnumLikeContainer[SqlColumn]):
    item_type = SqlColumn


class SqlColumnsWithID(SqlColumns):
    ID = SqlColumn("id", SqlDataTypes.INTEGER, primary_key=True, autoincrement=True)
