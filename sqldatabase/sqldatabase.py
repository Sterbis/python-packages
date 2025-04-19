import re
import sqlite3
from pathlib import Path
from typing import Any, Generic, TypeVar

import sqlparse  # type: ignore

from .sqlbase import SqlBase
from .sqlcolumn import SqlColumn
from .sqlcondition import SqlBaseCondition
from .sqlfunction import SqlAggregateFunction
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
    name: str
    tables: T

    def __init__(
        self,
        name: str | None = None,
        tables: T | None = None,
        file_path: str | Path | None = None,
    ) -> None:
        if name is None:
            assert hasattr(
                self, "name"
            ), (
                "Database name must be specified either as class attribute"
                " or passed when instance is created."
            )
        else:
            self.name = name
        if tables is None:
            assert hasattr(
                self, "tables"
            ), (
                "Database tables must be specified either as class attribute"
                "or passed when instance is created."
            )
        else:
            self.tables = tables
        self.file_path = (
            Path(f"{self.name}.db").resolve() if file_path is None else Path(file_path)
        )
        file_exists = self.file_path.is_file()
        self._connection = sqlite3.connect(self.file_path)
        self._cursor = self._connection.cursor()
        if not file_exists:
            self._create_tables()

    def _create_table(self, table: SqlTable) -> None:
        create_table_statement = SqlCreateTableStatement(table)
        self._execute_statement(create_table_statement)

    def _create_tables(self) -> None:
        for table in self.tables:
            self._create_table(table)
        self._connection.commit()

    def _replace_sql_parameters(self, sql: str, parameters: dict[str, Any]) -> str:
        def format_value(value: Any) -> str:
            if isinstance(value, str):
                value = value.replace("'", "''")
                return f"'{value}'"
            elif value is None:
                return "NULL"
            else:
                return str(value)

        def get_value(match: re.Match) -> str:
            parameter = match.group("PARAMETER")
            if parameter not in parameters:
                raise ValueError(f"Missing value for SQL parameter: {parameter}\n{sql}")
            return format_value(parameters[parameter])

        return re.sub(r":(?P<PARAMETER>\w+)", get_value, sql)

    def _format_sql(self, sql: str, parameters: dict[str, Any]) -> str:
        sql = self._replace_sql_parameters(sql, parameters)
        return sqlparse.format(sql, keyword_case="upper", reindent=True)

    def _execute_statement(self, statement: SqlStatement) -> None:
        sql = statement.to_sql()
        parameters = statement.parameters
        self.execute(sql, parameters)

    def to_sql(self) -> str:
        return self.name

    def execute(self, sql: str, parameters: dict[str, Any] | None = None) -> None:
        parameters = parameters or {}
        # print(self._format_sql(sql, parameters))  # Debugging: Uncomment if needed
        self._cursor.execute(sql, parameters)

    def delete_records(
        self,
        table: SqlTable,
        where_condition: SqlBaseCondition,
    ) -> list[int] | None:
        delete_statement = SqlDeleteStatement(
            table=table, where_condition=where_condition
        )
        self._execute_statement(delete_statement)
        if self._cursor.description is not None:
            return [row[0] for row in self._cursor.fetchall()]
        return None

    def insert_record(
        self, table: SqlTable, record: dict[SqlColumn, Any]
    ) -> int | None:
        insert_statement = SqlInsertIntoStatement(table=table, record=record)
        self._execute_statement(insert_statement)
        return self._cursor.lastrowid

    def select_records(
        self,
        table: SqlTable,
        items: list[SqlColumn | SqlAggregateFunction] | None = None,
        where_condition: SqlBaseCondition | None = None,
        joins: list[SqlJoin] | None = None,
        group_by_columns: list[SqlColumn] | None = None,
        having_condition: SqlBaseCondition | None = None,
        order_by_columns: list[SqlColumn] | None = None,
        distinct: bool = False,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict[SqlColumn | SqlAggregateFunction | str, Any]]:
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
        self._execute_statement(select_statement)
        records = []
        for row in self._cursor.fetchall():
            if items is None:
                records.append(
                    {column[0]: row[index] for index, column in enumerate(self._cursor.description)}
                )
            else:
                if len(items) == len(row):
                    records.append({item: row[index] for index, item in enumerate(items)})
                else:
                    raise ValueError("Mismatch between items and row lengths.")
        return records

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
        self._execute_statement(update_statement)
        if self._cursor.description is not None:
            return [row[0] for row in self._cursor.fetchall()]
        return None
