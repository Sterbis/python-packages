from abc import ABC, abstractmethod
from enum import Enum
from typing import Any


class SqlBase(ABC):
    def __str__(self) -> str:
        return self.to_sql()

    @abstractmethod
    def to_sql(self) -> str:
        pass


class SqlBaseEnum(Enum):
    def __str__(self) -> str:
        return self.to_sql()

    def to_sql(self) -> str:
        return self.value


def value_to_sql(value: Any) -> str:
    if isinstance(value, str):
        value = value.replace("'", "''")
        return f"'{value}'"
    elif value is None:
        return "NULL"
    else:
        return str(value)
