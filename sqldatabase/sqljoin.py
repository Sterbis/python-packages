from __future__ import annotations
from typing import TYPE_CHECKING

from .sqlbase import SqlBase, SqlBaseEnum
from .sqlcondition import SqlCondition
from .sqloperator import ESqlComparisonOperator

if TYPE_CHECKING:
    from .sqlcolumn import SqlColumn
    from .sqltable import SqlTable


class ESqlJoinType(SqlBaseEnum):
    """
    Enumeration for SQL join types.

    Attributes:
        CROSS (str): Represents a CROSS JOIN.
        FULL (str): Represents a FULL JOIN.
        INNER (str): Represents an INNER JOIN.
        LEFT (str): Represents a LEFT JOIN.
        RIGHT (str): Represents a RIGHT JOIN.
    """
    CROSS = "CROSS"
    FULL = "FULL"
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"


class SqlJoin(SqlBase):
    """
    Represents a SQL JOIN clause.

    Attributes:
        table (SqlTable): The table to join.
        type (ESqlJoinType): The type of join (e.g., INNER, LEFT).
        condition (SqlCondition): The condition for the join.
    """
    def __init__(
        self,
        table: SqlTable,
        *columns: SqlColumn,
        type_: ESqlJoinType = ESqlJoinType.INNER,
        operator: ESqlComparisonOperator = ESqlComparisonOperator.IS_EQUAL,
    ) -> None:
        """
        Initialize a SqlJoin instance.

        Args:
            table (SqlTable): The table to join.
            columns (SqlColumn): The columns involved in the join condition.
            type_ (ESqlJoinType, optional): The type of join. Defaults to INNER.
            operator (ESqlComparisonOperator, optional): The comparison operator. Defaults to IS_EQUAL.
        """
        self.table = table
        self.type = type_
        self.condition = SqlCondition(columns[0], operator, *columns[1:])

    def to_sql(self) -> str:
        """
        Convert the join clause to its SQL representation.

        Returns:
            str: The SQL representation of the join clause.
        """
        return f"{self.type} JOIN {self.table.fully_qualified_name} ON {self.condition}"
