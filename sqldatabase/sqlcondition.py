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
        self.item = item
        self.values = values
        self.operator = operator
        self.values_to_sql: list[str] = []
        self.parameters: dict[str, Any] = {}
        self._verify_value_count()
        self._parse_values()
        if isinstance(self.item, SqlSelectStatement):
            self.parameters.update(self.item.parameters)

    def _verify_value_count(self) -> None:
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
            required_value_count is None or len(self.values) == required_value_count
        ), (
            f"Expected exactly {required_value_count} values for {self.operator} operator,"
            f" {len(self.values)} were given."
        )

    def _parse_values(self) -> None:
        from .sqlcolumn import SqlColumn
        from .sqlstatement import SqlSelectStatement

        item = (
            self.item.items[0]
            if isinstance(self.item, SqlSelectStatement)
            else self.item
        )
        adapter = item.adapter
        for value in self.values:
            if isinstance(value, SqlColumn):
                self.values_to_sql.append(value.fully_qualified_name)
            elif isinstance(value, SqlAggregateFunction):
                self.values_to_sql.append(value.to_sql())
            elif isinstance(value, SqlSelectStatement):
                self.values_to_sql.append(f"({value.to_sql()})")
                self.parameters.update(value.parameters)
            else:
                parameter = self.item.parameter_name
                if adapter is not None:
                    value = adapter(value)
                self.values_to_sql.append(f":{parameter}")
                self.parameters[parameter] = value

    def to_sql(self) -> str:
        from .sqlcolumn import SqlColumn
        from .sqlstatement import SqlSelectStatement

        if isinstance(self.item, SqlColumn):
            sql = self.item.fully_qualified_name
        elif isinstance(self.item, SqlAggregateFunction):
            sql = self.item.to_sql()
        elif isinstance(self.item, SqlSelectStatement):
            sql = f"({self.item.to_sql()})"
        else:
            assert False, f"Invalid item: {self.item}."

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
            lower_value, upper_value = self.values_to_sql
            return sql + f" {lower_value} AND {upper_value}"
        elif self.operator in (
            ESqlComparisonOperator.IS_IN,
            ESqlComparisonOperator.IS_NOT_IN,
        ):
            return sql + f" ({', '.join(self.values_to_sql)})"
        else:
            return sql + f" {self.values_to_sql[0]}"


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
