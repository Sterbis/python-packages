from __future__ import annotations
import uuid
from typing import TYPE_CHECKING, Any, Callable

from shared import EnumLikeClassContainer

from .sqlbase import SqlBase

if TYPE_CHECKING:
    from .sqlcolumn import SqlColumn


class SqlAggregateFunction(SqlBase):
    name: str

    def __init__(self, column: SqlColumn | None = None):
        assert hasattr(
            self, "name"
        ), "Function name must be specified as class attribute."
        self.column = column

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, SqlAggregateFunction)
            and self.name == other.name
            and self.column == other.column
        )

    def __hash__(self):
        return hash(self.name) + hash(self.column)

    @property
    def alias(self) -> str:
        if self.column is None:
            return f"FUNCTION.{self.name}"
        else:
            return f"FUNCTION.{self.name}.{self.column.alias}"

    @property
    def adapter(self) -> Callable[[Any], Any] | None:
        if self.column is not None:
            return self.column.adapter
        return None

    @property
    def converter(self) -> Callable[[Any], Any] | None:
        if self.column is not None:
            return self.column.converter
        return None

    @property
    def parameter_name(self) -> str:
        if self.column is None:
            return f"{self.name}_{uuid.uuid4().hex[:8]}"
        else:
            return f"{self.name}_{self.column.parameter_name}"

    def to_sql(self) -> str:
        if self.column is None:
            return f"{self.name.upper()}(*)"
        else:
            return f"{self.name.upper()}({self.column})"


class SqlCount(SqlAggregateFunction):
    name = "count"


class SqlAggregateFunctionWithMandatoryColumn(SqlAggregateFunction):
    def __init__(self, column: SqlColumn):
        SqlAggregateFunction.__init__(self, column)


class SqlMin(SqlAggregateFunctionWithMandatoryColumn):
    name = "min"


class SqlMax(SqlAggregateFunctionWithMandatoryColumn):
    name = "max"


class SqlSum(SqlAggregateFunctionWithMandatoryColumn):
    name = "sum"


class SqlAvg(SqlAggregateFunctionWithMandatoryColumn):
    name = "avg"


class SqlFunctions(EnumLikeClassContainer[SqlAggregateFunction]):
    item_type = SqlAggregateFunction

    AVG = SqlAvg
    COUNT = SqlCount
    MAX = SqlMax
    MIN = SqlMin
    SUM = SqlSum
