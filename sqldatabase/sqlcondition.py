from __future__ import annotations
from typing import TYPE_CHECKING, Any

from .sqlbase import SqlBase
from .sqlfunction import SqlAggregateFunction
from .sqloperator import ESqlComparisonOperator, ESqlLogicalOperator


if TYPE_CHECKING:
    from .sqlcolumn import SqlColumn
    from .sqlstatement import SqlSelectStatement


class SqlBaseCondition(SqlBase):
    parameters: dict[str, Any]

    def __and__(self, other: SqlBaseCondition):
        return SqlCompoundCondition(self, ESqlLogicalOperator.AND, other)

    def __or__(self, other: SqlBaseCondition):
        return SqlCompoundCondition(self, ESqlLogicalOperator.OR, other)


class SqlCondition(SqlBaseCondition):
    def __init__(
        self,
        item: SqlColumn | SqlAggregateFunction | SqlSelectStatement,
        *values: Any,
        operator: ESqlComparisonOperator = ESqlComparisonOperator.IS_EQUAL,
    ) -> None:
        from .sqlcolumn import SqlColumn
        from .sqlstatement import SqlSelectStatement

        self.item = item
        self.values = values
        self.operator = operator
        self.parameters = {
            f"{self.item.parameter_name}_value_{index}": value
            for index, value in enumerate(values)
            if not isinstance(value, (SqlColumn, SqlSelectStatement))
        }
        if isinstance(item, SqlSelectStatement):
            self.parameters.update(item.parameters)
        if len(self.values) == 1 and isinstance(self.values[0], SqlSelectStatement):
            self.parameters.update(self.values[0].parameters)

    def to_sql(self) -> str:
        from .sqlcolumn import SqlColumn
        from .sqlstatement import SqlSelectStatement

        if isinstance(self.item, SqlSelectStatement):
            sql = f"({self.item}) {self.operator}"
        else:
            sql = f"{self.item} {self.operator}"

        if len(self.values) == 1 and isinstance(self.values[0], SqlColumn):
            return sql + f" {self.values[0]}"
        elif len(self.values) == 1 and isinstance(self.values[0], SqlSelectStatement):
            return sql + f" ({self.values[0]})"
        elif self.operator in (
            ESqlComparisonOperator.IS_NULL,
            ESqlComparisonOperator.IS_NOT_NULL,
        ):
            return sql
        elif self.operator in (
            ESqlComparisonOperator.IS_BETWEEN,
            ESqlComparisonOperator.IS_NOT_BETWEEN,
        ):
            if len(self.parameters) != 2:
                raise ValueError("Expected exactly two parameters for BETWEEN operator.")
            lower_parameter, upper_parameter = self.parameters
            return sql + f" :{lower_parameter} AND :{upper_parameter}"
        elif self.operator in (
            ESqlComparisonOperator.IS_IN,
            ESqlComparisonOperator.IS_NOT_IN,
        ):
            return (
                sql
                + f" ({', '.join(f':{parameter}' for parameter in self.parameters)})"
            )
        else:
            parameter, *_ = self.parameters
            return sql + f" :{parameter}"


class SqlCompoundCondition(SqlBaseCondition):
    def __init__(
        self,
        left_condition: SqlBaseCondition,
        operator: ESqlLogicalOperator,
        right_condition: SqlBaseCondition,
    ) -> None:
        self.left_condition = left_condition
        self.operator: ESqlLogicalOperator = operator
        self.right_condition = right_condition
        self.parameters = left_condition.parameters | right_condition.parameters

    def to_sql(self) -> str:
        return f"({self.left_condition.to_sql()} {self.operator} {self.right_condition.to_sql()})"
