from __future__ import annotations
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from jinja2 import Environment, FileSystemLoader

from .sqlbase import SqlBase, SqlBaseEnum
from .sqlcolumn import SqlColumn
from .sqlfunction import SqlAggregateFunction
from .sqltranspiler import ESqlDialect, SqlTranspiler

if TYPE_CHECKING:
    from .sqlcondition import SqlCondition
    from .sqljoin import SqlJoin
    from .sqlrecord import SqlRecord
    from .sqltable import SqlTable


class ESqlOrderByType(SqlBaseEnum):
    ASCENDING = "ASC"
    DESCENDING = "DESC"


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
        self.context["dialect"] = dialect.value
        self.context["parameters"] = parameters
        self._template_parameters = parameters or {}
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


class SqlDropTableStatement(SqlStatement):
    _template_file = "drop_table_statement.sql.j2"

    def __init__(
        self, dialect: ESqlDialect, table: SqlTable, if_exists: bool = False
    ) -> None:
        SqlStatement.__init__(self, dialect, table=table, if_exists=if_exists)


class SqlInsertIntoStatement(SqlStatement):
    _template_file = "insert_into_statement.sql.j2"

    def __init__(
        self,
        dialect: ESqlDialect,
        table: SqlTable,
        record: SqlRecord,
    ) -> None:
        parameters = record.to_database_parameters()
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
        is_subquery: bool = False,
    ) -> None:
        parameters = {}
        if where_condition:
            parameters.update(where_condition.parameters)
        if having_condition:
            parameters.update(having_condition.parameters)
        preprocessed_items = self._preprocess_items(table, *items)
        preprocessed_order_by_items = self._preprocess_order_by_items(order_by_items)

        SqlStatement.__init__(
            self,
            dialect,
            parameters,
            table=table,
            items=preprocessed_items,
            where_condition=where_condition,
            joins=joins,
            group_by_columns=group_by_columns,
            having_condition=having_condition,
            order_by_items=preprocessed_order_by_items,
            distinct=distinct,
            limit=limit,
            offset=offset,
            is_subquery=is_subquery,
        )

    @staticmethod
    def _preprocess_items(
        table: SqlTable, *items: SqlColumn | SqlAggregateFunction
    ) -> list[SqlColumn | SqlAggregateFunction]:
        if len(items) == 0:
            preprocessed_items = list(table.columns)
        else:
            preprocessed_items = list(items)
        return preprocessed_items

    @staticmethod
    def _preprocess_order_by_items(
        order_by_items: list[SqlColumn | SqlAggregateFunction | ESqlOrderByType] | None,
    ) -> list[tuple[SqlColumn | SqlAggregateFunction, ESqlOrderByType | None]] | None:
        if order_by_items is not None:
            preprocessed_order_by_items: list[
                tuple[SqlColumn | SqlAggregateFunction, ESqlOrderByType | None]
            ] = []
            index = 0
            while index < len(order_by_items):
                item = order_by_items[index]
                assert isinstance(
                    item, (SqlColumn, SqlAggregateFunction)
                ), f"Unexpected order by item type {type(item)} with index {index}."
                order = None
                if index + 1 < len(order_by_items):
                    next_item = order_by_items[index + 1]
                    if isinstance(next_item, ESqlOrderByType):
                        order = next_item
                        index += 1
                preprocessed_order_by_items.append((item, order))
                index += 1
            return preprocessed_order_by_items
        return None

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
        parameters = record.to_database_parameters()
        parameters.update(where_condition.parameters)
        columns_and_parameters = list(zip(record.keys(), parameters))
        SqlStatement.__init__(
            self,
            dialect,
            parameters,
            table=table,
            columns_and_parameters=columns_and_parameters,
            where_condition=where_condition,
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
