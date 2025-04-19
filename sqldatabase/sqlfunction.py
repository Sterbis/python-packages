from __future__ import annotations
from typing import TYPE_CHECKING

from .sqlbase import SqlBase

if TYPE_CHECKING:
    from .sqlcolumn import SqlColumn


class SqlAggregateFunction(SqlBase):
    name: str

    def __init__(self, column: SqlColumn | None = None):
        self.column = column

    @property
    def parameter_name(self) -> str:
        if self.column is None:
            return self.name
        else:
            return f"{self.name}_{self.column.parameter_name}"

    def to_sql(self) -> str:
        if self.column is None:
            return f"{self.name}(*)"
        else:
            return f"{self.name}({self.column})"


class SqlCount(SqlAggregateFunction):
    name = "COUNT"


class SqlAggregateFunctionWithMandatoryColumn(SqlAggregateFunction):
    def __init__(self, column: SqlColumn):
        SqlAggregateFunction.__init__(self, column)


class SqlMin(SqlAggregateFunctionWithMandatoryColumn):
    name = "MIN"


class SqlMax(SqlAggregateFunctionWithMandatoryColumn):
    name = "MAX"


class SqlSum(SqlAggregateFunctionWithMandatoryColumn):
    name = "SUM"


class SqlAvg(SqlAggregateFunctionWithMandatoryColumn):
    name = "AVG"
