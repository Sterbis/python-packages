from __future__ import annotations
import uuid
from typing import TYPE_CHECKING, Any, Callable

from shared import EnumLikeClassContainer

from .sqlbase import SqlBase
from .sqldatatype import SqlDataType

if TYPE_CHECKING:
    from .sqlcolumn import SqlColumn


class SqlAggregateFunction(SqlBase):
    """
    Represents a SQL aggregate function (e.g., COUNT, SUM).

    Attributes:
        name (str): The name of the aggregate function.
        column (SqlColumn | None): The column the function operates on.
    """

    name: str

    def __init__(self, column: SqlColumn | None = None):
        """
        Initialize a SqlAggregateFunction instance.

        Args:
            column (SqlColumn | None, optional): The column the function operates on. Defaults to None.
        """
        assert hasattr(
            self, "name"
        ), "Function name must be specified as class attribute."
        self.column = column

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, SqlAggregateFunction)
            and self.fully_qualified_name == other.fully_qualified_name
        )

    def __hash__(self):
        return hash(self.fully_qualified_name)

    @property
    def alias(self) -> str:
        """
        Get the alias for the aggregate function.

        Returns:
            str: The alias for the function.
        """
        if self.column is None:
            return f"FUNCTION.{self.name}"
        else:
            return f"FUNCTION.{self.name}.{self.column.alias}"

    @property
    def fully_qualified_name(self) -> str:
        """
        Get the fully qualified name of the aggregate function.

        Returns:
            str: The fully qualified name of the function.
        """
        if self.column is None:
            return f"{self.name.upper()}(*)"
        else:
            return f"{self.name.upper()}({self.column.fully_qualified_name})"

    @property
    def to_database_converter(self) -> Callable[[Any], Any] | None:
        if self.column is not None:
            return self.column.to_database_converter
        return None

    @property
    def from_database_converter(self) -> Callable[[Any], Any] | None:
        if self.column is not None:
            return self.column.from_database_converter
        return None

    @property
    def data_type(self) -> SqlDataType | None:
        if self.column is not None:
            return self.column.data_type
        return None

    def generate_parameter_name(self) -> str:
        if self.column is None:
            return f"{self.name}_{uuid.uuid4().hex[:8]}"
        else:
            return f"{self.name}_{self.column.generate_parameter_name()}"

    def to_sql(self) -> str:
        if self.column is None:
            return f"{self.name.upper()}(*)"
        else:
            return f"{self.name.upper()}({self.column})"


class SqlCount(SqlAggregateFunction):
    name = "count"


class SqlAggregateFunctionWithMandatoryColumn(SqlAggregateFunction):
    """Represents a SQL aggregate function that requires a column."""

    def __init__(self, column: SqlColumn):
        """Initialize a SqlAggregateFunctionWithMandatoryColumn instance.

        Args:
            column (SqlColumn): The column the function operates on.
        """
        SqlAggregateFunction.__init__(self, column)


class SqlMin(SqlAggregateFunctionWithMandatoryColumn):
    """Represents the SQL MIN aggregate function."""

    name = "min"


class SqlMax(SqlAggregateFunctionWithMandatoryColumn):
    """Represents the SQL MAX aggregate function."""

    name = "max"


class SqlSum(SqlAggregateFunctionWithMandatoryColumn):
    """Represents the SQL SUM aggregate function."""

    name = "sum"


class SqlAvg(SqlAggregateFunctionWithMandatoryColumn):
    """Represents the SQL AVG aggregate function."""

    name = "avg"


class SqlFunctions(EnumLikeClassContainer[SqlAggregateFunction]):
    """Container for managing multiple SQL aggregate functions."""

    item_type = SqlAggregateFunction

    AVG = SqlAvg
    COUNT = SqlCount
    MAX = SqlMax
    MIN = SqlMin
    SUM = SqlSum
