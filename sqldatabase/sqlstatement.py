from __future__ import annotations
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

from .sqlbase import SqlBase
from .sqlcolumn import SqlColumn
from .sqlcondition import SqlCondition
from .sqlfunction import SqlAggregateFunction
from .sqljoin import SqlJoin
from .sqlrecord import SqlRecord
from .sqltable import SqlTable
from .sqltranspiler import ESqlDialect, SqlTranspiler


class SqlStatement(SqlBase):
    _environment = Environment(
        loader=FileSystemLoader(Path(__file__).parent / "templates")
    )
    _template_dialect = ESqlDialect.SQLITE
    _template_file: str

    def __init__(
        self,
        dialect: ESqlDialect,
        parameters: dict[str, Any] | None = None,
        **context,
    ) -> None:
        self.dialect = dialect
        self.context = context
        self.context["parameters"] = parameters
        self._template_parameters = parameters
        self._template_sql = self._render_template()

    @property
    def sql(self) -> str:
        return SqlTranspiler(self.dialect).transpile_sql(
            self._template_sql,
            self._template_dialect,
            pretty=True,
        )

    @property
    def parameters(self) -> dict[str, Any] | Sequence:
        return SqlTranspiler(self.dialect).transpile_parameters(
            self._template_sql,
            self._template_parameters,
            self._template_dialect,
        )

    def _render_template(self) -> str:
        template = self._environment.get_template(self._template_file)
        template_sql = template.render(self.context)
        template_sql = "\n".join(
            line for line in template_sql.splitlines() if line.strip()
        )
        return template_sql

    def to_sql(self) -> str:
        return self.sql


class SqlCreateTableStatement(SqlStatement):
    _template_file = "create_table_statement.sql.j2"

    def __init__(
        self, dialect: ESqlDialect, table: SqlTable, if_not_exists: bool = False
    ) -> None:
        SqlStatement.__init__(self, dialect, table=table, if_not_exists=if_not_exists)


class SqlInsertIntoStatement(SqlStatement):
    _template_file = "insert_into_statement.sql.j2"

    def __init__(
        self,
        dialect: ESqlDialect,
        table: SqlTable,
        record: SqlRecord,
    ) -> None:
        parameters = record.generate_parameters()
        columns = list(record.keys())
        SqlStatement.__init__(
            self,
            dialect,
            parameters,
            table=table,
            columns=columns,
        )


class SqlSelectStatement(SqlStatement):
    _template_file = "select_statement.sql.j2"

    def __init__(
        self,
        dialect: ESqlDialect,
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
        is_subquery: bool = False,
    ) -> None:
        parameters = {}
        if where_condition:
            parameters.update(where_condition.parameters)
        if having_condition:
            parameters.update(having_condition.parameters)
        if items is None:
            items = list(table.columns)
        elif isinstance(items, Sequence):
            items = list(items)
        else:
            items = [items]
        SqlStatement.__init__(
            self,
            dialect,
            parameters,
            table=table,
            items=items,
            where_condition=where_condition,
            joins=joins,
            group_by_columns=group_by_columns,
            having_condition=having_condition,
            order_by_columns=order_by_columns,
            distinct=distinct,
            limit=limit,
            offset=offset,
            is_subquery=is_subquery,
        )

    def generate_parameter_name(self) -> str:
        assert (
            len(self.context["items"]) == 1
        ), "Select statement must return exactly one value when compared with parameter value."
        return f"SELECT_{self.context['items'][0].generate_parameter_name()}"


class SqlUpdateStatement(SqlStatement):
    _template_file = "update_statement.sql.j2"

    def __init__(
        self,
        dialect: ESqlDialect,
        table: SqlTable,
        record: SqlRecord,
        where_condition: SqlCondition,
    ) -> None:
        parameters = record.generate_parameters()
        parameters.update(where_condition.parameters)
        columns = list(record.keys())
        SqlStatement.__init__(
            self,
            dialect,
            parameters,
            table=table,
            where_condition=where_condition,
            columns=columns,
        )


class SqlDeleteStatement(SqlStatement):
    _template_file = "delete_statement.sql.j2"

    def __init__(
        self,
        dialect: ESqlDialect,
        table: SqlTable,
        where_condition: SqlCondition,
    ) -> None:
        parameters = where_condition.parameters
        SqlStatement.__init__(
            self,
            dialect,
            parameters,
            table=table,
            where_condition=where_condition,
        )
