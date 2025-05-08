from __future__ import annotations
from typing import TYPE_CHECKING, Any, Iterable

from .sqlcondition import SqlCondition
from .sqloperator import ESqlComparisonOperator

if TYPE_CHECKING:
    from .sqlcolumn import SqlColumn


class SqlColumnFilter(SqlCondition):
    operator: ESqlComparisonOperator

    def __init__(self, column: SqlColumn, *values) -> None:
        SqlCondition.__init__(self, column, self.operator, *values)
        self.column = column


class ValueColumnFilter(SqlColumnFilter):
    def __init__(self, column: SqlColumn, value: Any) -> None:
        SqlColumnFilter.__init__(self, column, value)
        self.value = value


class IsEqualColumnFilter(ValueColumnFilter):
    operator = ESqlComparisonOperator.IS_EQUAL


class IsNotEqualColumnFilter(ValueColumnFilter):
    operator = ESqlComparisonOperator.IS_NOT_EQUAL


class IsLessThanColumnFilter(ValueColumnFilter):
    operator = ESqlComparisonOperator.IS_LESS_THAN


class IsLessThanOrEqualColumnFilter(ValueColumnFilter):
    operator = ESqlComparisonOperator.IS_LESS_THAN_OR_EQUAL


class IsGreaterThanColumnFilter(ValueColumnFilter):
    operator = ESqlComparisonOperator.IS_GREATER_THAN


class IsGreaterThanOrEqualColumnFilter(ValueColumnFilter):
    operator = ESqlComparisonOperator.IS_GREATER_THAN_OR_EQUAL


class IsLikeColumnFilter(ValueColumnFilter):
    operator = ESqlComparisonOperator.IS_LIKE


class IsNotLikeColumnFilter(ValueColumnFilter):
    operator = ESqlComparisonOperator.IS_NOT_LIKE


class ValuesColumnFilter(SqlColumnFilter):
    def __init__(self, column: SqlColumn, values: Iterable) -> None:
        SqlColumnFilter.__init__(self, column, *values)


class IsInColumnFilter(ValuesColumnFilter):
    operator = ESqlComparisonOperator.IS_IN


class IsNotInColumnFilter(ValuesColumnFilter):
    operator = ESqlComparisonOperator.IS_NOT_IN


class BetweenColumnFilter(SqlColumnFilter):
    def __init__(self, column: SqlColumn, lower_value: Any, upper_value: Any):
        SqlColumnFilter.__init__(self, column, lower_value, upper_value)
        self.lower_value = lower_value
        self.upper_value = upper_value


class IsBetweenColumnFilter(BetweenColumnFilter):
    operator = ESqlComparisonOperator.IS_BETWEEN


class IsNotBetweenColumnFilter(BetweenColumnFilter):
    operator = ESqlComparisonOperator.IS_NOT_BETWEEN


class NullColumnFilter(SqlColumnFilter):
    def __init__(self, column: SqlColumn):
        SqlColumnFilter.__init__(self, column)


class IsNullColumnFilter(SqlColumnFilter):
    operator = ESqlComparisonOperator.IS_NULL


class IsNotNullColumnFilter(SqlColumnFilter):
    operator = ESqlComparisonOperator.IS_NOT_NULL


class SqlColumnFilters:
    def __init__(self, column: SqlColumn) -> None:
        self.column = column

    def IS_BETWEEN(self, lower_value: Any, upper_value: Any) -> IsBetweenColumnFilter:
        return IsBetweenColumnFilter(self.column, lower_value, upper_value)

    def IS_EQUAL(self, value: Any) -> IsEqualColumnFilter:
        return IsEqualColumnFilter(self.column, value)

    def IS_GREATER_THAN(self, value: Any) -> IsGreaterThanColumnFilter:
        return IsGreaterThanColumnFilter(self.column, value)

    def IS_GREATER_THAN_OR_EQUAL(self, value: Any) -> IsGreaterThanOrEqualColumnFilter:
        return IsGreaterThanOrEqualColumnFilter(self.column, value)

    def IS_IN(self, values: Iterable) -> IsInColumnFilter:
        return IsInColumnFilter(self.column, values)

    def IS_LESS_THAN(self, value: Any) -> IsLessThanColumnFilter:
        return IsLessThanColumnFilter(self.column, value)

    def IS_LESS_THAN_OR_EQUAL(self, value: Any) -> IsLessThanOrEqualColumnFilter:
        return IsLessThanOrEqualColumnFilter(self.column, value)

    def IS_LIKE(self, value: Any) -> IsLikeColumnFilter:
        return IsLikeColumnFilter(self.column, value)

    def IS_NOT_BETWEEN(
        self, lower_value: Any, upper_value: Any
    ) -> IsNotBetweenColumnFilter:
        return IsNotBetweenColumnFilter(self.column, lower_value, upper_value)

    def IS_NOT_EQUAL(self, value: Any) -> IsNotEqualColumnFilter:
        return IsNotEqualColumnFilter(self.column, value)

    def IS_NOT_IN(self, values: Iterable) -> IsNotInColumnFilter:
        return IsNotInColumnFilter(self.column, values)

    def IS_NOT_LIKE(self, value: Any) -> IsNotLikeColumnFilter:
        return IsNotLikeColumnFilter(self.column, value)

    def IS_NOT_NULL(self) -> IsNotNullColumnFilter:
        return IsNotNullColumnFilter(self.column)

    def IS_NULL(self) -> IsNullColumnFilter:
        return IsNullColumnFilter(self.column)
