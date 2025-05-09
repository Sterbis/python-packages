from __future__ import annotations
from typing import TYPE_CHECKING

from .sqlbase import SqlBase, SqlBaseEnum
from .sqlcondition import SqlCondition
from .sqloperator import ESqlComparisonOperator

if TYPE_CHECKING:
    from .sqlcolumn import SqlColumn
    from .sqltable import SqlTable


class ESqlJoinType(SqlBaseEnum):
    CROSS = "CROSS"
    FULL = "FULL"
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"


class SqlJoin(SqlBase):
    def __init__(
        self,
        table: SqlTable,
        *columns: SqlColumn,
        type_: ESqlJoinType = ESqlJoinType.INNER,
        operator: ESqlComparisonOperator = ESqlComparisonOperator.IS_EQUAL,
    ) -> None:
        self.table = table
        self.type = type_
        self.condition = SqlCondition(columns[0], operator, *columns[1:])

    def to_sql(self) -> str:
        return f"{self.type} JOIN {self.table.fully_qualified_name} ON {self.condition}"
