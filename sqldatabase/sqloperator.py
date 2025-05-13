from .sqlbase import SqlBaseEnum


class ESqlComparisonOperator(SqlBaseEnum):
    """
    Enumeration for SQL comparison operators.

    Attributes:
        IS_EQUAL (str): Represents the '=' operator.
        IS_NOT_EQUAL (str): Represents the '!=' operator.
        IS_GREATER_THAN (str): Represents the '>' operator.
        IS_LESS_THAN (str): Represents the '<' operator.
        IS_GREATER_THAN_OR_EQUAL (str): Represents the '>=' operator.
        IS_LESS_THAN_OR_EQUAL (str): Represents the '<=' operator.
    """
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
    """
    Enumeration for SQL logical operators.

    Attributes:
        AND (str): Represents the 'AND' operator.
        OR (str): Represents the 'OR' operator.
    """
    AND = "AND"
    OR = "OR"
