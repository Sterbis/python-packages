from .sqlbase import SqlBase, SqlBaseEnum
from .sqlcolumn import SqlColumn
from .sqlcondition import SqlCondition
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
        # self.condition = SqlCondition(columns[0], *columns[1:], operator)

    def to_sql(self) -> str:
        # return f"{self.type} JOIN {self.table.fully_qualified_name} ON {self.condition.to_sql()}"
        sql = f"{self.type} JOIN {self.table.fully_qualified_name} ON "
        if self.operator in (
            ESqlComparisonOperator.IS_NULL,
            ESqlComparisonOperator.IS_NOT_NULL,
        ):
            return sql + f"{self.columns[0].fully_qualified_name} {self.operator}"
        elif self.operator in (
            ESqlComparisonOperator.IS_BETWEEN,
            ESqlComparisonOperator.IS_NOT_BETWEEN,
        ):
            return (
                sql
                + f"{self.columns[0].fully_qualified_name}"
                + f" {self.operator} {self.columns[1].fully_qualified_name}"
                + f" AND {self.columns[2].fully_qualified_name}"
            )
        elif self.operator in (
            ESqlComparisonOperator.IS_IN,
            ESqlComparisonOperator.IS_NOT_IN,
        ):
            return (
                sql
                + f"{self.columns[0].fully_qualified_name} {self.operator}"
                + f" ({', '.join(f'{column.fully_qualified_name}' for column in self.columns[1:])})"
            )
        else:
            return (
                sql
                + f"{self.columns[0].fully_qualified_name}"
                + f" {self.operator} {self.columns[1].fully_qualified_name}"
            )
