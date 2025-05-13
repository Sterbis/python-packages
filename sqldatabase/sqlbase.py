from abc import ABC, abstractmethod
from enum import Enum
from typing import Any


class SqlBase(ABC):
    """
    Abstract base class for SQL-related objects.

    Methods:
        to_sql: Abstract method to convert the object to its SQL representation.
    """
    def __str__(self) -> str:
        return self.to_sql()

    @abstractmethod
    def to_sql(self) -> str:
        """
        Convert the object to its SQL representation.

        Returns:
            str: The SQL representation of the object.
        """
        pass


class SqlBaseEnum(Enum):
    """
    Base class for SQL-related enumerations.

    Methods:
        to_sql: Convert the enumeration value to its SQL representation.
    """
    def __str__(self) -> str:
        return self.to_sql()

    def to_sql(self) -> str:
        """
        Convert the enumeration value to its SQL representation.

        Returns:
            str: The SQL representation of the enumeration value.
        """
        return self.value


def value_to_sql(value: Any) -> str:
    """
    Convert a Python value to its SQL representation.

    Args:
        value (Any): The value to convert.

    Returns:
        str: The SQL representation of the value.
    """
    if isinstance(value, str):
        value = value.replace("'", "''")
        return f"'{value}'"
    elif value is None:
        return "NULL"
    else:
        return str(value)
