from __future__ import annotations
import pprint
import sqlite3
import textwrap
from collections.abc import Sequence
from typing import Any, Generic, TypeVar

import pyodbc  # type: ignore

from .sqlbase import SqlBase
from .sqlcolumn import SqlColumn
from .sqlcondition import SqlCondition
from .sqldatatype import SqlDataTypes
from .sqlfunction import (
    SqlAggregateFunction,
    SqlAggregateFunctionWithMandatoryColumn,
    SqlFunctions,
)
from .sqljoin import SqlJoin
from .sqlrecord import SqlRecord
from .sqlstatement import (
    SqlCreateTableStatement,
    SqlDeleteStatement,
    SqlInsertIntoStatement,
    SqlSelectStatement,
    SqlStatement,
    SqlUpdateStatement,
)
from .sqltable import SqlTable, SqlTables
from .sqltranspiler import ESqlDialect


T = TypeVar("T", bound=SqlTables)


class SqlDatabase(SqlBase, Generic[T]):
    data_types: SqlDataTypes
    dialect: ESqlDialect
    tables: T

    def __init__(
        self,
        name: str,
        connection: sqlite3.Connection | pyodbc.Connection,
        tables: T | None = None,
    ) -> None:
        self.name = name
        self._connection = connection
        if tables is None:
            assert hasattr(self, "tables"), (
                "Database tables must be specified either as class attribute"
                "or passed when instance is created."
            )
            self.tables = self.tables
        else:
            self.tables = tables
        for data_type in self.data_types:
            data_type.database = self
        for table in self.tables:
            table.database = self
            for column in table.columns:
                column.data_type = self.data_types(column.data_type.name)
        self.functions = SqlFunctions()
        self.attached_databases: dict[str, SqlDatabase] = {}

    @property
    def autocommit(self) -> bool:
        return bool(self._connection.autocommit)

    def _fetch_records(self, cursor: sqlite3.Cursor | pyodbc.Cursor) -> list[SqlRecord]:
        def parse_alias(
            alias: str,
        ) -> tuple[str | None, str | None, str | None, str | None]:
            function_name = database_name = table_name = column_name = None
            alias_parts = alias.split(".")
            if alias_parts[0] == "FUNCTION":
                function_name = alias_parts[1]
                if len(alias_parts) == 6:
                    database_name = alias_parts[3]
                    table_name = alias_parts[4]
                    column_name = alias_parts[5]
            elif alias_parts[0] == "COLUMN":
                database_name = alias_parts[1]
                table_name = alias_parts[2]
                column_name = alias_parts[3]
            return function_name, database_name, table_name, column_name

        def get_item(alias) -> SqlColumn | SqlAggregateFunction:
            function_name, database_name, table_name, column_name = parse_alias(alias)
            if (
                database_name is not None
                and table_name is not None
                and column_name is not None
            ):
                database = (
                    self
                    if database_name == self.name
                    else self.attached_databases[database_name]
                )
                column = database.tables(table_name).columns(column_name)
            else:
                column = None
            if function_name is not None:
                function_class = self.functions(function_name)
                assert not (
                    column is None
                    and issubclass(
                        function_class, SqlAggregateFunctionWithMandatoryColumn
                    )
                ), f"Invalid alias format: {alias}"
                item = function_class(column)
            elif column is not None:
                item = column
            else:
                assert False, f"Invalid alias format: {alias}"
            return item

        records: list[SqlRecord] = []
        if cursor.description is not None:
            aliases = [description[0] for description in cursor.description]
            for row in cursor.fetchall():
                record = SqlRecord()
                for alias, value in zip(aliases, row):
                    item = get_item(alias)
                    if (
                        item.data_type is not None
                        and item.data_type.converter is not None
                    ):
                        value = item.data_type.converter(value)
                    if item.converter is not None:
                        value = item.converter(value)
                    record[item] = value
                records.append(record)
        return records

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
        return self._connection.execute(sql, parameters)

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()

    def create_table(self, table: SqlTable, if_not_exists: bool = False) -> None:
        create_table_statement = SqlCreateTableStatement(
            self.dialect, table, if_not_exists
        )
        self._execute_statement(create_table_statement)

    def create_all_tables(self, if_not_exists: bool = False) -> None:
        for table in self.tables:
            self.create_table(table, if_not_exists)
        self.commit()

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
            if index > 0 and insert_statement._template_parameters is not None:
                for parameter, value in zip(
                    insert_statement._template_parameters.keys(), record.values()
                ):
                    insert_statement._template_parameters[parameter] = value
            cursor = self._execute_statement(insert_statement)
            if cursor.description is not None:
                row = cursor.fetchone()
                if row is not None:
                    ids.append(row[0])
        return ids if len(ids) else None

    def select_records(
        self,
        table: SqlTable,
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
        select_statement = SqlSelectStatement(
            self.dialect,
            table,
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
        if cursor.description is not None:
            return [row[0] for row in cursor.fetchall()]
        return None

    def delete_records(
        self,
        table: SqlTable,
        where_condition: SqlCondition,
    ) -> list[int] | None:
        delete_statement = SqlDeleteStatement(self.dialect, table, where_condition)
        cursor = self._execute_statement(delete_statement)
        if cursor.description is not None:
            return [row[0] for row in cursor.fetchall()]
        return None

    def record_count(self, table: SqlTable) -> int:
        count_function = self.functions.COUNT()
        records = self.select_records(table, count_function)
        return records[0][count_function]
