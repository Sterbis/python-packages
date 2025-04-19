from .sqlbase import SqlBaseEnum


class ESqlComparisonOperator(SqlBaseEnum):
    IS_BETWEEN = "BETWEEN"
    IS_EQUAL = "="
    IS_GREATER_THAN = ">"
    IS_GREATER_THAN_OR_EQUAL = "<="
    IS_IN = "IN"
    IS_LESS_THAN = "<"
    IS_LESS_THAN_OR_EQUAL = "<="
    IS_LIKE = "LIKE"
    IS_NOT_BETWEEN = "NOT BETWEEN"
    IS_NOT_EQUAL = "!="
    IS_NOT_IN = "NOT IN"
    IS_NOT_LIKE = "NOT LIKE"
    IS_NOT_NULL = "IS NOT NULL"
    IS_NULL = "IS NULL"


class ESqlLogicalOperator(SqlBaseEnum):
    AND = "AND"
    OR = "OR"
