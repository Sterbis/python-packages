from __future__ import annotations
import copy
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Generic, Generator, TypeVar

from shared import EnumLikeContainer

from .sqlbase import SqlBase
from .sqlcolumn import SqlColumn, SqlColumns
from .sqlcondition import SqlBaseCondition
from .sqlfunction import SqlAggregateFunction

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
        else:
            self.columns = columns
        for column in self.columns:
            column.table = self

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, SqlTable)
            and self.name == other.name
            and self.database == other.database
        )

    def __deepcopy__(self, memo):
        cls = self.__class__
        table = cls.__new__(cls)
        memo[id(self)] = table
        for name, value in self.__dict__.items():
            if name not in ("database"):
                setattr(table, name, copy.deepcopy(value))
        for column in table.columns:
            column.table = table
        return table

    def __hash__(self):
        return hash(self.name) + hash(self.database)

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

    def delete_records(
        self,
        where_condition: SqlBaseCondition,
    ) -> list[int] | None:
        return self.database.delete_records(self, where_condition)

    def insert_record(self, record: dict[SqlColumn, Any]) -> int | None:
        return self.database.insert_record(self, record)

    def select_records(
        self,
        items: (
            SqlColumn
            | SqlAggregateFunction
            | Sequence[SqlColumn | SqlAggregateFunction]
            | None
        ) = None,
        where_condition: SqlBaseCondition | None = None,
        joins: list[SqlJoin] | None = None,
        group_by_columns: list[SqlColumn] | None = None,
        having_condition: SqlBaseCondition | None = None,
        order_by_columns: list[SqlColumn] | None = None,
        distinct: bool = False,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict[SqlColumn | SqlAggregateFunction, Any]]:
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
        self, record: dict[SqlColumn, Any], where_condition: SqlBaseCondition
    ) -> list[int] | None:
        return self.database.update_records(self, record, where_condition)


class SqlTables(EnumLikeContainer[SqlTable]):
    item_type = SqlTable
