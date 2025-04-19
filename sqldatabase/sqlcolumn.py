from __future__ import annotations
from enum import Enum
from typing import TYPE_CHECKING, Generator

from .sqlbase import SqlBase
from .sqlcolumnfilter import SqlColumnFilters
from .sqldatatype import ESqlDataType

if TYPE_CHECKING:
    from .sqltable import SqlTable


class SqlColumn(SqlBase):
    table: SqlTable

    def __init__(
        self,
        name: str,
        data_type: ESqlDataType,
        unique: bool = False,
        primary_key: bool = False,
        foreign_key: bool = False,
        reference_column: SqlColumn | None = None,
        values: type[Enum] | None = None,
    ):
        assert (
            not foreign_key or reference_column is not None
        ), "Reference column must be specified for foreign key column."
        self.name = name
        self.data_type = data_type
        self.unique = unique
        self.primary_key = primary_key
        self.foreign_key = foreign_key
        self.reference_column = reference_column
        self.values = values
        self.filters = SqlColumnFilters(self)

    @property
    def parameter_name(self) -> str:
        return f"{self.table.name}_{self.name}"

    def to_sql(self) -> str:
        return f"{self.table.name}.{self.name}"


class SqlColumns:
    @classmethod
    def __iter__(cls) -> Generator[SqlColumn]:
        for value in cls.__dict__.values():
            if isinstance(value, SqlColumn):
                yield value
