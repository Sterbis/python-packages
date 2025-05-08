from __future__ import annotations
import copy
from collections.abc import Sequence
from typing import TYPE_CHECKING, Generic, TypeVar

from shared import EnumLikeContainer

from .sqlbase import SqlBase
from .sqlcolumn import SqlColumn, SqlColumns
from .sqlcondition import SqlCondition
from .sqlfunction import SqlAggregateFunction
from .sqlrecord import SqlRecord

if TYPE_CHECKING:
    from .sqldatabase import SqlDatabase
    from .sqljoin import SqlJoin


T = TypeVar("T", bound=SqlColumns)


class SqlTable(SqlBase, Generic[T]):
    name: str
    columns: T
    database: SqlDatabase

    def __init__(self, name: str | None = None, columns: T | None = None):
        if name is None:
            assert hasattr(self, "name"), (
                "Table name must be specified either as class attribute"
                " or passed when instance is created."
            )
        else:
            self.name = name
        if columns is None:
            assert hasattr(self, "columns"), (
                "Table columns must be specified either as class attribute"
                " or passed when instance is created."
            )
            self.columns = self.columns
        else:
            self.columns = columns
        for column in self.columns:
            column.table = self

    def __deepcopy__(self, memo) -> SqlTable:
        cls = self.__class__
        table = cls.__new__(cls)
        memo[id(self)] = table
        for name, value in self.__dict__.items():
            if name not in ("database"):
                setattr(table, name, copy.deepcopy(value))
        for column in table.columns:
            column.table = table
        return table

    @property
    def primary_key_column(self) -> SqlColumn | None:
        for column in self.columns:
            if column.primary_key:
                return column
        return None

    @property
    def foreign_key_columns(self) -> list[SqlColumn]:
        return [column for column in self.columns if column.reference is not None]

    @property
    def fully_qualified_name(self) -> str:
        return f"{self.database.name}.{self.name}"

    def to_sql(self) -> str:
        return self.name

    def insert_records(
        self,
        records: SqlRecord | Sequence[SqlRecord],
    ) -> list[int] | None:
        return self.database.insert_records(self, records)

    def select_records(
        self,
        items: (
            SqlColumn
            | SqlAggregateFunction
            | Sequence[SqlColumn | SqlAggregateFunction]
            | None
        ) = None,
        where_condition: SqlCondition | None = None,
        joins: list[SqlJoin] | None = None,
        group_by_columns: list[SqlColumn] | None = None,
        having_condition: SqlCondition | None = None,
        order_by_columns: list[SqlColumn] | None = None,
        distinct: bool = False,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[SqlRecord]:
        return self.database.select_records(
            self,
            items,
            where_condition,
            joins,
            group_by_columns,
            having_condition,
            order_by_columns,
            distinct,
            limit,
            offset,
        )

    def update_records(
        self, record: SqlRecord, where_condition: SqlCondition
    ) -> list[int] | None:
        return self.database.update_records(self, record, where_condition)

    def delete_records(
        self,
        where_condition: SqlCondition,
    ) -> list[int] | None:
        return self.database.delete_records(self, where_condition)

    def record_count(self) -> int:
        return self.database.record_count(self)


class SqlTables(EnumLikeContainer[SqlTable]):
    item_type = SqlTable
