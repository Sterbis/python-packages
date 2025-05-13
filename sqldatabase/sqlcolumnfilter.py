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
    """
    Represents filters that can be applied to a SQL column.

    Attributes:
        column (SqlColumn): The column to which the filters are applied.
    """
    def __init__(self, column: SqlColumn):
        """
        Initialize a SqlColumnFilters instance.

        Args:
            column (SqlColumn): The column to which the filters are applied.
        """
        self.column = column

    def IS_BETWEEN(self, lower_value: Any, upper_value: Any) -> IsBetweenColumnFilter:
        """
        Create an IS_BETWEEN filter.

        Args:
            lower_value (Any): The lower bound value.
            upper_value (Any): The upper bound value.

        Returns:
            IsBetweenColumnFilter: The created filter.
        """
        return IsBetweenColumnFilter(self.column, lower_value, upper_value)

    def IS_EQUAL(self, value: Any) -> IsEqualColumnFilter:
        """
        Create an IS_EQUAL filter.

        Args:
            value (Any): The value to compare.

        Returns:
            IsEqualColumnFilter: The created filter.
        """
        return IsEqualColumnFilter(self.column, value)

    def IS_GREATER_THAN(self, value: Any) -> IsGreaterThanColumnFilter:
        """
        Create an IS_GREATER_THAN filter.

        Args:
            value (Any): The value to compare.

        Returns:
            IsGreaterThanColumnFilter: The created filter.
        """
        return IsGreaterThanColumnFilter(self.column, value)

    def IS_GREATER_THAN_OR_EQUAL(self, value: Any) -> IsGreaterThanOrEqualColumnFilter:
        """
        Create an IS_GREATER_THAN_OR_EQUAL filter.

        Args:
            value (Any): The value to compare.

        Returns:
            IsGreaterThanOrEqualColumnFilter: The created filter.
        """
        return IsGreaterThanOrEqualColumnFilter(self.column, value)

    def IS_IN(self, values: Iterable) -> IsInColumnFilter:
        """
        Create an IS_IN filter.

        Args:
            values (Iterable): The values to compare.

        Returns:
            IsInColumnFilter: The created filter.
        """
        return IsInColumnFilter(self.column, values)

    def IS_LESS_THAN(self, value: Any) -> IsLessThanColumnFilter:
        """
        Create an IS_LESS_THAN filter.

        Args:
            value (Any): The value to compare.

        Returns:
            IsLessThanColumnFilter: The created filter.
        """
        return IsLessThanColumnFilter(self.column, value)

    def IS_LESS_THAN_OR_EQUAL(self, value: Any) -> IsLessThanOrEqualColumnFilter:
        """
        Create an IS_LESS_THAN_OR_EQUAL filter.

        Args:
            value (Any): The value to compare.

        Returns:
            IsLessThanOrEqualColumnFilter: The created filter.
        """
        return IsLessThanOrEqualColumnFilter(self.column, value)

    def IS_LIKE(self, value: Any) -> IsLikeColumnFilter:
        """
        Create an IS_LIKE filter.

        Args:
            value (Any): The value to compare.

        Returns:
            IsLikeColumnFilter: The created filter.
        """
        return IsLikeColumnFilter(self.column, value)

    def IS_NOT_BETWEEN(
        self, lower_value: Any, upper_value: Any
    ) -> IsNotBetweenColumnFilter:
        """
        Create an IS_NOT_BETWEEN filter.

        Args:
            lower_value (Any): The lower bound value.
            upper_value (Any): The upper bound value.

        Returns:
            IsNotBetweenColumnFilter: The created filter.
        """
        return IsNotBetweenColumnFilter(self.column, lower_value, upper_value)

    def IS_NOT_EQUAL(self, value: Any) -> IsNotEqualColumnFilter:
        """
        Create an IS_NOT_EQUAL filter.

        Args:
            value (Any): The value to compare.

        Returns:
            IsNotEqualColumnFilter: The created filter.
        """
        return IsNotEqualColumnFilter(self.column, value)

    def IS_NOT_IN(self, values: Iterable) -> IsNotInColumnFilter:
        """
        Create an IS_NOT_IN filter.

        Args:
            values (Iterable): The values to compare.

        Returns:
            IsNotInColumnFilter: The created filter.
        """
        return IsNotInColumnFilter(self.column, values)

    def IS_NOT_LIKE(self, value: Any) -> IsNotLikeColumnFilter:
        """
        Create an IS_NOT_LIKE filter.

        Args:
            value (Any): The value to compare.

        Returns:
            IsNotLikeColumnFilter: The created filter.
        """
        return IsNotLikeColumnFilter(self.column, value)

    def IS_NOT_NULL(self) -> IsNotNullColumnFilter:
        """
        Create an IS_NOT_NULL filter.

        Returns:
            IsNotNullColumnFilter: The created filter.
        """
        return IsNotNullColumnFilter(self.column)

    def IS_NULL(self) -> IsNullColumnFilter:
        """
        Create an IS_NULL filter.

        Returns:
            IsNullColumnFilter: The created filter.
        """
        return IsNullColumnFilter(self.column)
