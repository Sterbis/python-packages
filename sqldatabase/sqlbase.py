from abc import ABC, abstractmethod
from enum import Enum


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
