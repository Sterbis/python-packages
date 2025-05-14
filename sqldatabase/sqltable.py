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
    """Represents a SQL table.

    Attributes:
        name (str): The name of the table.
        columns (T): The columns in the table.
        database (SqlDatabase): The database the table belongs to.
    """

    name: str
    columns: T
    database: SqlDatabase

    def __init__(
        self,
        name: str | None = None,
        schema_name: str | None = None,
        columns: T | None = None,
    ):
        """Initialize a SqlTable instance.

        Args:
            name (str | None, optional): The name of the table. Defaults to None.
            schema_name (str | None, optional): The schema name of the table. Defaults to None.
            columns (T | None, optional): The columns in the table. Defaults to None.
        """
        if name is None:
            assert hasattr(self.__class__, "name"), (
                "Table name must be specified either as class attribute"
                " or passed when instance is created."
            )
            self.name = self.__class__.name
        else:
            self.name = name
        self._schema_name = schema_name
        if columns is None:
            assert hasattr(self.__class__, "columns"), (
                "Table columns must be specified either as class attribute"
                " or passed when instance is created."
            )
            self.columns = self.__class__.columns
        else:
            self.columns = columns
        for column in self.columns:
            column.table = self

    def __deepcopy__(self, memo) -> SqlTable:
        """Create a deep copy of the table.

        Args:
            memo (dict): A dictionary of objects already copied during the current copying pass.

        Returns:
            SqlTable: A deep copy of the table.
        """
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
    def fully_qualified_name(self) -> str:
        """Get the fully qualified name of the table.

        Returns:
            str: The fully qualified name of the table.
        """
        return self.database.get_table_fully_qualified_name(self)

    @property
    def schema_name(self) -> str | None:
        """Get the schema name of the table.

        Returns:
            str | None: The schema name of the table.
        """
        return self._schema_name or self.database.default_schema_name

    @property
    def primary_key_column(self) -> SqlColumn | None:
        """Get the primary key column of the table.

        Returns:
            SqlColumn | None: The primary key column of the table.
        """
        for column in self.columns:
            if column.primary_key:
                return column
        return None

    @property
    def foreign_key_columns(self) -> list[SqlColumn]:
        """Get the foreign key columns of the table.

        Returns:
            list[SqlColumn]: The foreign key columns of the table.
        """
        return [column for column in self.columns if column.reference is not None]

    @property
    def referenced_tables(self) -> list[SqlTable]:
        """Get the tables referenced by the foreign key columns of the table.

        Returns:
            list[SqlTable]: The tables referenced by the foreign key columns of the table.
        """
        return [
            column.reference.table
            for column in self.columns
            if column.reference is not None and column.reference.table is not None
        ]

    def to_sql(self) -> str:
        """Convert the table to its SQL representation.

        Returns:
            str: The SQL representation of the table.
        """
        return self.name

    def get_column(self, column_name) -> SqlColumn:
        """Get a column by name.

        Args:
            column_name (str): The name of the column.

        Returns:
            SqlColumn: The column with the specified name.

        Raises:
            AssertionError: If the column is not found.
        """
        for column in self.columns:
            if column.name == column_name:
                return column
        assert False, f"Column '{column_name}' not found in table '{self.name}'."

    def get_foreign_key_column(self, table: SqlTable) -> SqlColumn | None:
        """Get the foreign key column that references the specified table.

        Args:
            table (SqlTable): The table to find the foreign key column for.

        Returns:
            SqlColumn | None: The foreign key column that references the specified table, or None if not found.
        """
        for foreign_key_column in self.foreign_key_columns:
            if foreign_key_column.reference in table.columns:
                return foreign_key_column
        return None

    def join(
        self, table: SqlTable, join_type: ESqlJoinType = ESqlJoinType.INNER
    ) -> SqlJoin:
        """Create a join with another table.

        Args:
            table (SqlTable): The table to join with.
            join_type (ESqlJoinType, optional): The type of join. Defaults to ESqlJoinType.INNER.

        Returns:
            SqlJoin: The join with the specified table.

        Raises:
            AssertionError: If no foreign key column is found to join the tables.
        """
        foreign_key_column = table.get_foreign_key_column(
            self
        ) or self.get_foreign_key_column(table)
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
        """Insert records into the table.

        Args:
            records (SqlRecord | Sequence[SqlRecord]): The records to insert.

        Returns:
            list[int] | None: The IDs of the inserted records, or None if the operation failed.
        """
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
        """Select records from the table.

        Args:
            *items (SqlColumn | SqlAggregateFunction): The columns or aggregate functions to select.
            where_condition (SqlCondition | None, optional): The condition to filter the records. Defaults to None.
            joins (list[SqlJoin] | None, optional): The joins to apply. Defaults to None.
            group_by_columns (list[SqlColumn] | None, optional): The columns to group by. Defaults to None.
            having_condition (SqlCondition | None, optional): The condition to filter the groups. Defaults to None.
            order_by_items (list[SqlColumn | SqlAggregateFunction | ESqlOrderByType] | None, optional): The items to order by. Defaults to None.
            distinct (bool, optional): Whether to select distinct records. Defaults to False.
            limit (int | None, optional): The maximum number of records to return. Defaults to None.
            offset (int | None, optional): The number of records to skip. Defaults to None.

        Returns:
            list[SqlRecord]: The selected records.
        """
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
        """Update records in the table.

        Args:
            record (SqlRecord): The record with updated values.
            where_condition (SqlCondition): The condition to filter the records to update.

        Returns:
            list[int] | None: The IDs of the updated records, or None if the operation failed.
        """
        return self.database.update_records(self, record, where_condition)

    def delete_records(
        self,
        where_condition: SqlCondition,
    ) -> list[int] | None:
        """Delete records from the table.

        Args:
            where_condition (SqlCondition): The condition to filter the records to delete.

        Returns:
            list[int] | None: The IDs of the deleted records, or None if the operation failed.
        """
        return self.database.delete_records(self, where_condition)

    def record_count(self) -> int:
        """Get the count of records in the table.

        Returns:
            int: The count of records in the table.
        """
        return self.database.record_count(self)


class SqlTables(EnumLikeContainer[SqlTable]):
    """Container for managing multiple SQL tables."""

    item_type = SqlTable
