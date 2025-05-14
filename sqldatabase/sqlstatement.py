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
    """Enumeration for SQL ORDER BY types.

    Attributes:
        ASCENDING (str): Represents ascending order.
        DESCENDING (str): Represents descending order.
    """

    ASCENDING = "ASC"
    DESCENDING = "DESC"


class SqlStatement(SqlBase):
    """Represents a SQL statement.

    Attributes:
        dialect (ESqlDialect): The SQL dialect for the statement.
        context (dict): The context for rendering the statement.
        template_parameters (dict[str, Any]): Parameters for the statement template.
        template_sql (str): The rendered SQL template.
    """

    _environment = Environment(
        loader=FileSystemLoader(Path(__file__).parent / "templates")
    )
    template_dialect = ESqlDialect.SQLITE
    template_file: str

    def __init__(
        self,
        dialect: ESqlDialect,
        parameters: dict[str, Any] | None = None,
        **context,
    ) -> None:
        """Initialize a SqlStatement instance.

        Args:
            dialect (ESqlDialect): The SQL dialect for the statement.
            parameters (dict[str, Any] | None, optional): Parameters for the statement template. Defaults to None.
            context (dict): Additional context for rendering the statement.
        """
        self.dialect = dialect
        self.context = context
        self.context["dialect"] = dialect.value
        self.context["parameters"] = parameters
        self.template_parameters = parameters or {}
        self.template_sql = self._render_template()

    @property
    def sql(self) -> str:
        """Get the SQL representation of the statement.

        Returns:
            str: The SQL representation of the statement.
        """
        return SqlTranspiler(self.dialect).transpile_sql(
            self.template_sql,
            self.template_dialect,
            pretty=True,
        )

    @property
    def parameters(self) -> dict[str, Any] | Sequence:
        """Get the parameters for the statement.

        Returns:
            dict[str, Any] | Sequence: The parameters for the statement.
        """
        return SqlTranspiler(self.dialect).transpile_parameters(
            self.template_sql,
            self.template_parameters,
        )

    def _render_template(self) -> str:
        """Render the SQL template.

        Returns:
            str: The rendered SQL template.
        """
        template = self._environment.get_template(self.template_file)
        template_sql = template.render(self.context)
        template_sql = "\n".join(
            line for line in template_sql.splitlines() if line.strip()
        )
        return template_sql

    def to_sql(self) -> str:
        """Get the SQL representation of the statement.

        Returns:
            str: The SQL representation of the statement.
        """
        return self.sql


class SqlCreateTableStatement(SqlStatement):
    """Represents a SQL CREATE TABLE statement."""

    template_file = "create_table_statement.sql.j2"

    def __init__(
        self, dialect: ESqlDialect, table: SqlTable, if_not_exists: bool = False
    ) -> None:
        """Initialize a SqlCreateTableStatement instance.

        Args:
            dialect (ESqlDialect): The SQL dialect for the statement.
            table (SqlTable): The table to create.
            if_not_exists (bool, optional): Whether to include IF NOT EXISTS. Defaults to False.
        """
        SqlStatement.__init__(self, dialect, table=table, if_not_exists=if_not_exists)


class SqlDropTableStatement(SqlStatement):
    """Represents a SQL DROP TABLE statement."""

    template_file = "drop_table_statement.sql.j2"

    def __init__(
        self, dialect: ESqlDialect, table: SqlTable, if_exists: bool = False
    ) -> None:
        """Initialize a SqlDropTableStatement instance.

        Args:
            dialect (ESqlDialect): The SQL dialect for the statement.
            table (SqlTable): The table to drop.
            if_exists (bool, optional): Whether to include IF EXISTS. Defaults to False.
        """
        SqlStatement.__init__(self, dialect, table=table, if_exists=if_exists)


class SqlInsertIntoStatement(SqlStatement):
    """Represents a SQL INSERT INTO statement."""

    template_file = "insert_into_statement.sql.j2"

    def __init__(
        self,
        dialect: ESqlDialect,
        table: SqlTable,
        record: SqlRecord,
    ) -> None:
        """Initialize a SqlInsertIntoStatement instance.

        Args:
            dialect (ESqlDialect): The SQL dialect for the statement.
            table (SqlTable): The table to insert into.
            record (SqlRecord): The record to insert.
        """
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
    """Represents a SQL SELECT statement."""

    template_file = "select_statement.sql.j2"

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
        """Initialize a SqlSelectStatement instance.

        Args:
            dialect (ESqlDialect): The SQL dialect for the statement.
            table (SqlTable): The table to select from.
            *items (SqlColumn | SqlAggregateFunction): The columns or aggregate functions to select.
            where_condition (SqlCondition | None, optional): The WHERE condition. Defaults to None.
            joins (list[SqlJoin] | None, optional): The JOIN clauses. Defaults to None.
            group_by_columns (list[SqlColumn] | None, optional): The GROUP BY columns. Defaults to None.
            having_condition (SqlCondition | None, optional): The HAVING condition. Defaults to None.
            order_by_items (list[SqlColumn | SqlAggregateFunction | ESqlOrderByType] | None, optional): The ORDER BY items. Defaults to None.
            distinct (bool, optional): Whether to include DISTINCT. Defaults to False.
            limit (int | None, optional): The LIMIT value. Defaults to None.
            offset (int | None, optional): The OFFSET value. Defaults to None.
            is_subquery (bool, optional): Whether the statement is a subquery. Defaults to False.
        """
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
        """Preprocess the items to select.

        Args:
            table (SqlTable): The table to select from.
            *items (SqlColumn | SqlAggregateFunction): The columns or aggregate functions to select.

        Returns:
            list[SqlColumn | SqlAggregateFunction]: The preprocessed items.
        """
        if len(items) == 0:
            preprocessed_items = list(table.columns)
        else:
            preprocessed_items = list(items)
        return preprocessed_items

    @staticmethod
    def _preprocess_order_by_items(
        order_by_items: list[SqlColumn | SqlAggregateFunction | ESqlOrderByType] | None,
    ) -> list[tuple[SqlColumn | SqlAggregateFunction, ESqlOrderByType | None]] | None:
        """Preprocess the ORDER BY items.

        Args:
            order_by_items (list[SqlColumn | SqlAggregateFunction | ESqlOrderByType] | None): The ORDER BY items.

        Returns:
            list[tuple[SqlColumn | SqlAggregateFunction, ESqlOrderByType | None]] | None: The preprocessed ORDER BY items.
        """
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
        """Generate a unique parameter name for the SELECT statement.

        Returns:
            str: The generated parameter name.
        """
        assert (
            len(self.context["items"]) == 1
        ), "Select statement must return exactly one value when compared with parameter value."
        return f"SELECT_{self.context['items'][0].generate_parameter_name()}"


class SqlUpdateStatement(SqlStatement):
    """Represents a SQL UPDATE statement."""

    template_file = "update_statement.sql.j2"

    def __init__(
        self,
        dialect: ESqlDialect,
        table: SqlTable,
        record: SqlRecord,
        where_condition: SqlCondition,
    ) -> None:
        """Initialize a SqlUpdateStatement instance.

        Args:
            dialect (ESqlDialect): The SQL dialect for the statement.
            table (SqlTable): The table to update.
            record (SqlRecord): The record with updated values.
            where_condition (SqlCondition): The WHERE condition.
        """
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
    """Represents a SQL DELETE statement."""

    template_file = "delete_statement.sql.j2"

    def __init__(
        self,
        dialect: ESqlDialect,
        table: SqlTable,
        where_condition: SqlCondition,
    ) -> None:
        """Initialize a SqlDeleteStatement instance.

        Args:
            dialect (ESqlDialect): The SQL dialect for the statement.
            table (SqlTable): The table to delete from.
            where_condition (SqlCondition): The WHERE condition.
        """
        parameters = where_condition.parameters
        SqlStatement.__init__(
            self,
            dialect,
            parameters,
            table=table,
            where_condition=where_condition,
        )
