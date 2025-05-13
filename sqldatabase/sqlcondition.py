from __future__ import annotations
from typing import TYPE_CHECKING, Any

from .sqlbase import SqlBase
from .sqlfunction import SqlAggregateFunction
from .sqloperator import ESqlComparisonOperator, ESqlLogicalOperator


if TYPE_CHECKING:
    from .sqlcolumn import SqlColumn
    from .sqlstatement import SqlSelectStatement


class SqlCondition(SqlBase):
    """
    Represents a SQL condition used in WHERE, HAVING, or JOIN clauses.

    Attributes:
        left (SqlColumn | SqlAggregateFunction | SqlSelectStatement): The left-hand side of the condition.
        operator (ESqlComparisonOperator): The comparison operator.
        right (Any): The right-hand side of the condition.
        parameters (dict[str, Any]): Parameters for the condition.
        _values_to_sql (list[str]): SQL representations of the values.
    """
    def __init__(
        self,
        left: SqlColumn | SqlAggregateFunction | SqlSelectStatement,
        operator: ESqlComparisonOperator,
        *right: Any,
    ) -> None:
        """
        Initialize a SqlCondition instance.

        Args:
            left (SqlColumn | SqlAggregateFunction | SqlSelectStatement): The left-hand side of the condition.
            operator (ESqlComparisonOperator): The comparison operator.
            right (Any): The right-hand side of the condition.
        """
        from .sqlstatement import SqlSelectStatement

        self.left = left
        self.operator = operator
        self.right = right
        self.parameters: dict[str, Any] = {}
        self._values_to_sql: list[str] = []
        self._validate_value_count()
        self._parse_values()
        if isinstance(self.left, SqlSelectStatement):
            self.parameters.update(self.left.parameters)

    def __and__(self, other: SqlCondition):
        """
        Combine this condition with another using the AND logical operator.

        Args:
            other (SqlCondition): The other condition to combine.

        Returns:
            SqlCompoundCondition: A new compound condition.
        """
        return SqlCompoundCondition(self, ESqlLogicalOperator.AND, other)

    def __or__(self, other: SqlCondition):
        """
        Combine this condition with another using the OR logical operator.

        Args:
            other (SqlCondition): The other condition to combine.

        Returns:
            SqlCompoundCondition: A new compound condition.
        """
        return SqlCompoundCondition(self, ESqlLogicalOperator.OR, other)

    def _validate_value_count(self) -> None:
        if self.operator in (
            ESqlComparisonOperator.IS_NULL,
            ESqlComparisonOperator.IS_NOT_NULL,
        ):
            required_value_count = 0
        elif self.operator in (
            ESqlComparisonOperator.IS_BETWEEN,
            ESqlComparisonOperator.IS_NOT_BETWEEN,
        ):
            required_value_count = 2
        elif self.operator in (
            ESqlComparisonOperator.IS_IN,
            ESqlComparisonOperator.IS_NOT_IN,
        ):
            required_value_count = None
        else:
            required_value_count = 1
        assert (
            required_value_count is None or len(self.right) == required_value_count
        ), (
            f"Expected exactly {required_value_count} values for {self.operator} operator,"
            f" {len(self.right)} were given."
        )

    def _parse_values(self) -> None:
        from .sqlcolumn import SqlColumn
        from .sqlrecord import SqlRecord
        from .sqlstatement import SqlSelectStatement

        item = (
            self.left.context["items"][0]
            if isinstance(self.left, SqlSelectStatement)
            else self.left
        )
        for value in self.right:
            if isinstance(value, SqlColumn):
                self._values_to_sql.append(value.fully_qualified_name)
            elif isinstance(value, SqlAggregateFunction):
                self._values_to_sql.append(value.to_sql())
            elif isinstance(value, SqlSelectStatement):
                self._values_to_sql.append(f"({value.template_sql.rstrip(";")})")
                self.parameters.update(value.template_parameters)
            else:
                parameter = self.left.generate_parameter_name()
                self._values_to_sql.append(f":{parameter}")
                self.parameters[parameter] = SqlRecord.to_database_value(item, value)

    def to_sql(self) -> str:
        from .sqlcolumn import SqlColumn
        from .sqlstatement import SqlSelectStatement

        if isinstance(self.left, SqlColumn):
            sql = self.left.fully_qualified_name
        elif isinstance(self.left, SqlAggregateFunction):
            sql = self.left.to_sql()
        elif isinstance(self.left, SqlSelectStatement):
            sql = f"({self.left.to_sql()})"
        else:
            assert False, f"Invalid item: {self.left}."

        sql += f" {self.operator}"
        if self.operator in (
            ESqlComparisonOperator.IS_NULL,
            ESqlComparisonOperator.IS_NOT_NULL,
        ):
            return sql
        elif self.operator in (
            ESqlComparisonOperator.IS_BETWEEN,
            ESqlComparisonOperator.IS_NOT_BETWEEN,
        ):
            lower_value, upper_value = self._values_to_sql
            return sql + f" {lower_value} AND {upper_value}"
        elif self.operator in (
            ESqlComparisonOperator.IS_IN,
            ESqlComparisonOperator.IS_NOT_IN,
        ):
            return sql + f" ({', '.join(self._values_to_sql)})"
        else:
            return sql + f" {self._values_to_sql[0]}"


class SqlCompoundCondition(SqlCondition):
    def __init__(
        self,
        left: SqlCondition,
        operator: ESqlLogicalOperator,
        right: SqlCondition,
    ) -> None:
        self.left = left  # type: ignore
        self.operator: ESqlLogicalOperator = operator  # type: ignore
        self.right = right  # type: ignore
        self.parameters = left.parameters | right.parameters

    def to_sql(self) -> str:
        return f"({self.left} {self.operator} {self.right})"
