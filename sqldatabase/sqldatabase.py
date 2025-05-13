from __future__ import annotations
import copy
import pprint
import sqlite3
import textwrap
from abc import abstractmethod
from collections.abc import Sequence
from typing import Any, Generic, TypeVar

import pyodbc  # type: ignore

from .sqlbase import SqlBase
from .sqlcolumn import SqlColumn
from .sqlcondition import SqlCondition
from .sqldatatype import SqlDataType, SqlDataTypeWithParameter
from .sqlfunction import (
    SqlAggregateFunction,
    SqlFunctions,
)
from .sqljoin import SqlJoin
from .sqlrecord import SqlRecord
from .sqlstatement import (
    ESqlOrderByType,
    SqlCreateTableStatement,
    SqlDeleteStatement,
    SqlDropTableStatement,
    SqlInsertIntoStatement,
    SqlSelectStatement,
    SqlStatement,
    SqlUpdateStatement,
)
from .sqltable import SqlTable, SqlTables
from .sqltranspiler import ESqlDialect


T = TypeVar("T", bound=SqlTables)


class SqlDatabase(SqlBase, Generic[T]):
    dialect: ESqlDialect
    tables: T
    default_schema_name: str | None = None

    def __init__(
        self,
        name: str,
        connection: sqlite3.Connection | pyodbc.Connection,
        tables: T | None = None,
    ) -> None:
        self.name = name
        self._connection = connection
        if tables is None:
            assert hasattr(self.__class__, "tables"), (
                "Database tables must be specified either as class attribute"
                "or passed when instance is created."
            )
            self.tables = self.__class__.tables
        else:
            self.tables = tables
        self.functions = SqlFunctions()
        self.attached_databases: dict[str, SqlDatabase] = {}
        data_types = {}
        for table in self.tables:
            table.database = self
            for column in table.columns:
                assert isinstance(
                    column.data_type, SqlDataType
                ), f"Unexpected data type: {column.data_type}"
                if isinstance(column.data_type, SqlDataTypeWithParameter):
                    column.data_type.database = self
                else:
                    if column.data_type.name not in data_types:
                        data_type = copy.deepcopy(column.data_type)
                        data_type.database = self
                        data_types[data_type.name] = data_type
                    column.data_type = data_types[column.data_type.name]

    @property
    def autocommit(self) -> bool:
        return bool(self._connection.autocommit)

    @abstractmethod
    def _parse_table_fully_qualified_name(
        self,
        table_fully_qualified_name: str,
    ) -> tuple[str | None, str | None, str | None]:
        pass

    @abstractmethod
    def get_table_fully_qualified_name(self, table: SqlTable) -> str:
        pass

    def _fetch_records(self, cursor: sqlite3.Cursor | pyodbc.Cursor) -> list[SqlRecord]:
        records: list[SqlRecord] = []
        if cursor.description is not None:
            aliases = [description[0] for description in cursor.description]
            for row in cursor.fetchall():
                records.append(SqlRecord.from_database_row(aliases, row, self))
        return records

    def _fetch_ids(self, cursor: sqlite3.Cursor | pyodbc.Cursor) -> list[int] | None:
        if cursor.description is not None:
            return [row[0] for row in cursor.fetchall()]
        return None

    def _execute_statement(
        self, statement: SqlStatement
    ) -> sqlite3.Cursor | pyodbc.Cursor:
        return self.execute(statement.sql, statement.parameters)

    def to_sql(self) -> str:
        return self.name

    def execute(
        self,
        sql: str,
        parameters: dict[str, Any] | Sequence = (),
    ) -> sqlite3.Cursor | pyodbc.Cursor:
        print("=" * 80)
        print("Executing SQL:")
        print("-" * 80)
        print(textwrap.indent(sql, "  "))
        print()
        print(
            textwrap.indent(
                f"parameters = {pprint.pformat(parameters, sort_dicts=False)}", "  "
            )
        )
        print()
        return self._connection.execute(sql, parameters)

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()

    def create_table(self, table: SqlTable, if_not_exists: bool = False) -> None:
        self._execute_statement(
            SqlCreateTableStatement(self.dialect, table, if_not_exists)
        )

    def create_all_tables(self, if_not_exists: bool = False) -> None:
        for table in self.tables:
            self.create_table(table, if_not_exists)
        self.commit()

    def drop_table(self, table: SqlTable, if_exists: bool = False) -> None:
        self._execute_statement(SqlDropTableStatement(self.dialect, table, if_exists))

    def drop_all_tables(self, if_exists: bool = False) -> None:
        unsorted_tables = set(self.tables)
        sorted_tables: list[SqlTable] = []

        while unsorted_tables:
            referenced_tables = set()
            for table in unsorted_tables:
                for referenced_table in table.referenced_tables:
                    referenced_tables.add(referenced_table)
            unreferenced_tables = set(unsorted_tables) - set(referenced_tables)
            sorted_tables.extend(unreferenced_tables)
            unsorted_tables = referenced_tables

        for table in sorted_tables:
            self.drop_table(table, if_exists)

        self.commit()

    def get_table(self, table_fully_qualified_name: str) -> SqlTable:
        database_name, schema_name, table_name = self._parse_table_fully_qualified_name(
            table_fully_qualified_name
        )
        if database_name is not None and database_name != self.name:
            database = self.attached_databases[database_name]
        else:
            database = self
        for table in database.tables:
            if table.name == table_name and table.schema_name == schema_name:
                return table
        assert (
            False
        ), f"Table '{table_name}', schema '{schema_name}', not found in database '{database.name}'."

    def insert_records(
        self,
        table: SqlTable,
        records: SqlRecord | Sequence[SqlRecord],
    ) -> list[int] | None:
        if isinstance(records, SqlRecord):
            records = [records]

        insert_statement = SqlInsertIntoStatement(self.dialect, table, records[0])
        ids = []
        for index, record in enumerate(records):
            if index > 0 and insert_statement.template_parameters is not None:
                for parameter, value in zip(
                    insert_statement.template_parameters.keys(), record.values()
                ):
                    insert_statement.template_parameters[parameter] = value
            cursor = self._execute_statement(insert_statement)
            if cursor.description is not None:
                row = cursor.fetchone()
                if row is not None:
                    ids.append(row[0])
        return ids if len(ids) else None

    def select_records(
        self,
        table: SqlTable,
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
        select_statement = SqlSelectStatement(
            self.dialect,
            table,
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
        cursor = self._execute_statement(select_statement)
        return self._fetch_records(cursor)

    def update_records(
        self,
        table: SqlTable,
        record: SqlRecord,
        where_condition: SqlCondition,
    ) -> list[int] | None:
        update_statement = SqlUpdateStatement(
            self.dialect,
            table,
            record,
            where_condition,
        )
        cursor = self._execute_statement(update_statement)
        return self._fetch_ids(cursor)

    def delete_records(
        self,
        table: SqlTable,
        where_condition: SqlCondition,
    ) -> list[int] | None:
        delete_statement = SqlDeleteStatement(self.dialect, table, where_condition)
        cursor = self._execute_statement(delete_statement)
        return self._fetch_ids(cursor)

    def record_count(self, table: SqlTable) -> int:
        count_function = self.functions.COUNT()
        records = self.select_records(table, count_function)
        return records[0][count_function]
