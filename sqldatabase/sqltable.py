from __future__ import annotations
import copy
from collections.abc import Sequence
from typing import TYPE_CHECKING, Generic, TypeVar

from shared import EnumLikeContainer

from .sqlbase import SqlBase
from .sqlcolumn import SqlColumn, SqlColumns
from .sqlcondition import SqlCondition
from .sqlfunction import SqlAggregateFunction
from .sqljoin import ESqlJoinType, SqlJoin

if TYPE_CHECKING:
    from .sqldatabase import SqlDatabase
    from .sqlstatement import ESqlOrderByType
    from .sqlrecord import SqlRecord


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

    def get_foreign_key(self, table: SqlTable) -> SqlColumn | None:
        for foreign_key_column in self.foreign_key_columns:
            if foreign_key_column.reference in table.columns:
                return foreign_key_column
        return None

    def join(
        self, table: SqlTable, join_type: ESqlJoinType = ESqlJoinType.INNER
    ) -> SqlJoin:
        foreign_key_column = table.get_foreign_key(self) or self.get_foreign_key(table)
        assert foreign_key_column is not None, (
            f"Cannot join {self.fully_qualified_name} table with {table.fully_qualified_name} table."
            f" No foreign key column in {table.fully_qualified_name} table"
            f" referencing column in {self.fully_qualified_name} table"
            f" or foreign key column in {self.fully_qualified_name} table"
            f" referencing column in {table.fully_qualified_name} table found."
        )
        assert (
            foreign_key_column.reference is not None
        ), f"Invalid foreign key column: {foreign_key_column.fully_qualified_name}"
        return SqlJoin(
            table, foreign_key_column, foreign_key_column.reference, type_=join_type
        )

    def insert_records(
        self,
        records: SqlRecord | Sequence[SqlRecord],
    ) -> list[int] | None:
        return self.database.insert_records(self, records)

    def select_records(
        self,
        *items: SqlColumn | SqlAggregateFunction,
        where_condition: SqlCondition | None = None,
        joins: list[SqlJoin] | None = None,
        group_by_columns: list[SqlColumn] | None = None,
        having_condition: SqlCondition | None = None,
        order_by_items: (
            list[SqlColumn | SqlAggregateFunction | ESqlOrderByType] | None
        ) = None,
        distinct: bool = False,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[SqlRecord]:
        return self.database.select_records(
            self,
            *items,
            where_condition=where_condition,
            joins=joins,
            group_by_columns=group_by_columns,
            having_condition=having_condition,
            order_by_items=order_by_items,
            distinct=distinct,
            limit=limit,
            offset=offset,
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
