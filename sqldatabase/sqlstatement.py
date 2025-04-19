from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

from .sqlbase import SqlBase
from .sqlcolumn import SqlColumn
from .sqlcondition import SqlBaseCondition
from .sqlfunction import SqlAggregateFunction
from .sqljoin import SqlJoin
from .sqltable import SqlTable


class SqlStatement(SqlBase):
    template_file: str

    def __init__(self, parameters: dict[str, Any]) -> None:
        self.parameters = parameters
        self._environment = Environment(
            loader=FileSystemLoader(Path(__file__).parent / "templates")
        )

    def _render_template(self, template_file: str, **context) -> str:
        template = self._environment.get_template(template_file)
        return template.render(context)


class SqlCreateTableStatement(SqlStatement):
    template_file = "create_table_statement.sql.j2"

    def __init__(self, table: SqlTable) -> None:
        parameters: dict[str, Any] = {}
        SqlStatement.__init__(self, parameters)
        self.table = table

    def to_sql(self) -> str:
        return self._render_template(
            self.template_file,
            table=self.table,
        )


class SqlDeleteStatement(SqlStatement):
    template_file = "delete_statement.sql.j2"

    def __init__(
        self,
        table: SqlTable,
        where_condition: SqlBaseCondition,
    ) -> None:
        parameters = where_condition.parameters
        SqlStatement.__init__(self, parameters)
        self.table = table
        self.where_condition = where_condition

    def to_sql(self) -> str:
        return self._render_template(
            self.template_file,
            table=self.table,
            where_condition=self.where_condition,
        )


class SqlInsertIntoStatement(SqlStatement):
    template_file = "insert_into_statement.sql.j2"

    def __init__(
        self,
        table: SqlTable,
        record: dict[SqlColumn, Any],
    ) -> None:
        parameters = {
            f"{column.parameter_name}": value for column, value in record.items()
        }
        SqlStatement.__init__(self, parameters)
        self.table = table
        self.columns = list(record)

    def to_sql(self) -> str:
        return self._render_template(
            self.template_file, table=self.table, columns=self.columns
        )


class SqlSelectStatement(SqlStatement):
    template_file = "select_statement.sql.j2"

    def __init__(
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
        is_subquery: bool = False,
    ) -> None:
        parameters = {}
        if where_condition:
            parameters.update(where_condition.parameters)
        if having_condition:
            parameters.update(having_condition.parameters)
        SqlStatement.__init__(self, parameters)
        self.table = table
        self.items = items
        self.where_condition = where_condition
        self.joins = joins
        self.group_by_columns = group_by_columns
        self.having_condition = having_condition
        self.order_by_columns = order_by_columns
        self.distinct = distinct
        self.limit = limit
        self.offset = offset
        self.is_subquery = is_subquery

    @property
    def parameter_name(self) -> str:
        assert (
            self.items is not None and len(self.items) == 1
        ), "Select statement must return exactly one value when compared with parameter value."
        return f"SELECT_{self.items[0].parameter_name}"

    def to_sql(self) -> str:
        return self._render_template(
            self.template_file,
            table=self.table,
            items=self.items,
            where_condition=self.where_condition,
            joins=self.joins,
            group_by_columns=self.group_by_columns,
            having_condition=self.having_condition,
            order_by_columns=self.order_by_columns,
            distinct=self.distinct,
            limit=self.limit,
            offset=self.offset,
            is_subquery=self.is_subquery,
        )


class SqlUpdateStatement(SqlStatement):
    template_file = "update_statement.sql.j2"

    def __init__(
        self,
        table: SqlTable,
        record: dict[SqlColumn, Any],
        where_condition: SqlBaseCondition,
    ) -> None:
        parameters = {
            f"{column.parameter_name}": value for column, value in record.items()
        }
        parameters.update(where_condition.parameters)
        SqlStatement.__init__(self, parameters)
        self.table = table
        self.where_condition = where_condition
        self.columns = list(record)

    def to_sql(self) -> str:
        return self._render_template(
            self.template_file,
            table=self.table,
            columns=self.columns,
            where_condition=self.where_condition,
        )
