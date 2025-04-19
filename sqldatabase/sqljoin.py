from .sqlbase import SqlBase, SqlBaseEnum
from .sqlcolumn import SqlColumn
from .sqloperator import ESqlComparisonOperator
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
        self.columns = columns
        self.type = type_
        self.operator = operator

    def to_sql(self) -> str:
        sql = f"{self.type} JOIN {self.table} ON "
        if self.operator in (
            ESqlComparisonOperator.IS_NULL,
            ESqlComparisonOperator.IS_NOT_NULL,
        ):
            return sql + f"{self.columns[0]} {self.operator}"
        elif self.operator in (
            ESqlComparisonOperator.IS_BETWEEN,
            ESqlComparisonOperator.IS_NOT_BETWEEN,
        ):
            return (
                sql
                + f"{self.columns[0]} {self.operator} {self.columns[1]} AND {self.columns[2]}"
            )
        elif self.operator in (
            ESqlComparisonOperator.IS_IN,
            ESqlComparisonOperator.IS_NOT_IN,
        ):
            return (
                sql
                + f"{self.columns[0]} {self.operator} ({', '.join(f'{column}' for column in self.columns[1:])})"
            )
        else:
            return sql + f"{self.columns[0]} {self.operator} {self.columns[1]}"
