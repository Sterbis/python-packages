import pprint
import re
import sqlite3
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Generic, TypeVar

import sqlparse  # type: ignore

from .sqlbase import SqlBase, value_to_sql
from .sqlcolumn import SqlColumn
from .sqlcondition import SqlBaseCondition
from .sqlfunction import (
    SqlAggregateFunction,
    SqlAggregateFunctionWithMandatoryColumn,
    SqlFunctions,
)
from .sqljoin import SqlJoin
from .sqlstatement import (
    SqlCreateTableStatement,
    SqlDeleteStatement,
    SqlInsertIntoStatement,
    SqlSelectStatement,
    SqlStatement,
    SqlUpdateStatement,
)
from .sqltable import SqlTable, SqlTables


T = TypeVar("T", bound=SqlTables)


class SqlDatabase(SqlBase, Generic[T]):
    name = "main"
    tables: T

    def __init__(
        self,
        path: str | Path | None = None,
        name: str | None = None,
        tables: T | None = None,
        autocommit: bool = False,
    ) -> None:
        if name is not None:
            self.name = name
        if tables is None:
            assert hasattr(self, "tables"), (
                "Database tables must be specified either as class attribute"
                "or passed when instance is created."
            )
        else:
            self.tables = tables
        self.path = Path(f"{self.name}.db").resolve() if path is None else Path(path)
        database_exists = self.path.is_file()
        self._connection = sqlite3.connect(
            self.path, detect_types=sqlite3.PARSE_DECLTYPES, autocommit=autocommit
        )
        self._connection.row_factory = self._record_factory
        if not database_exists:
            self._create_tables()
        for table in self.tables:
            table.database = self
        self.functions = SqlFunctions()

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, SqlDatabase) and self.path == other.path

    def __hash__(self):
        return hash(self.path)

    def _record_factory(
        self, cursor: sqlite3.Cursor, row: tuple
    ) -> dict[SqlColumn | SqlAggregateFunction, Any]:
        def parse_alias(alias: str) -> tuple[str | None, str | None, str | None]:
            function_name = table_name = column_name = None
            alias_parts = alias.split(".")
            if alias_parts[0] == "FUNCTION":
                function_name = alias_parts[1]
                if len(alias_parts) == 5:
                    column_name = alias_parts[3]
                    table_name = alias_parts[4]
            elif alias_parts[0] == "COLUMN":
                column_name = alias_parts[1]
                table_name = alias_parts[2]
            return function_name, table_name, column_name

        def get_item(alias) -> SqlColumn | SqlAggregateFunction:
            function_name, table_name, column_name = parse_alias(alias)
            if table_name is not None and column_name is not None:
                column = self.tables(table_name).columns(column_name)
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
                raise ValueError(f"Invalid alias format: {alias}")
            return item

        aliases = [description[0] for description in cursor.description]
        record = {}
        for index, alias in enumerate(aliases):
            item = get_item(alias)
            value = row[index]
            if item.converter is not None:
                value = item.converter(value)
            record[item] = value
        return record

    def _create_table(self, table: SqlTable) -> None:
        create_table_statement = SqlCreateTableStatement(table)
        self._execute_statement(create_table_statement)

    def _create_tables(self) -> None:
        for table in self.tables:
            self._create_table(table)
        self._connection.commit()

    def _replace_sql_parameters(self, sql: str, parameters: dict[str, Any]) -> str:
        def get_value(match: re.Match) -> str:
            parameter = match.group("PARAMETER")
            if parameter not in parameters:
                raise ValueError(
                    f"Missing value for SQL parameter: {parameter}\n{sql}\n{parameters=}"
                )
            return value_to_sql(parameters[parameter])

        return re.sub(r":(?P<PARAMETER>\w+)", get_value, sql)

    def _format_sql(self, sql: str, parameters: dict[str, Any]) -> str:
        sql = self._replace_sql_parameters(sql, parameters)
        return sqlparse.format(sql, keyword_case="upper", reindent=True)

    def _execute_statement(self, statement: SqlStatement) -> sqlite3.Cursor:
        sql = statement.to_sql()
        parameters = statement.parameters
        return self.execute(sql, parameters)

    def to_sql(self) -> str:
        return self.name

    def execute(
        self, sql: str, parameters: dict[str, Any] | None = None
    ) -> sqlite3.Cursor:
        parameters = parameters or {}
        print(self._format_sql(sql, parameters))  # Debugging: Uncomment if needed
        print("-" * 100)
        print(f"parameters = {pprint.pformat(parameters)}")
        print("=" * 100)
        return self._connection.execute(sql, parameters)

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()

    def delete_records(
        self,
        table: SqlTable,
        where_condition: SqlBaseCondition,
    ) -> list[int] | None:
        delete_statement = SqlDeleteStatement(
            table=table, where_condition=where_condition
        )
        cursor = self._execute_statement(delete_statement)
        if cursor.description is not None:
            return [row[0] for row in cursor.fetchall()]
        return None

    def insert_record(
        self, table: SqlTable, record: dict[SqlColumn, Any]
    ) -> int | None:
        insert_statement = SqlInsertIntoStatement(table=table, record=record)
        cursor = self._execute_statement(insert_statement)
        return cursor.lastrowid

    def select_records(
        self,
        table: SqlTable,
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
        select_statement = SqlSelectStatement(
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
        return cursor.fetchall()

    def update_records(
        self,
        table: SqlTable,
        record: dict[SqlColumn, Any],
        where_condition: SqlBaseCondition,
    ) -> list[int] | None:
        update_statement = SqlUpdateStatement(
            table,
            record,
            where_condition,
        )
        cursor = self._execute_statement(update_statement)
        if cursor.description is not None:
            return [row[0] for row in cursor.fetchall()]
        return None
