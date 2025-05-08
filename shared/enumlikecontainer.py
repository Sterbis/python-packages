import copy
from typing import TypeVar, Generic, Type, Iterator, Any, Iterable, Callable


T = TypeVar("T")


class EnumLikeContainer(Generic[T]):
    item_type: Type[T]

    def __init__(self) -> None:
        print(f"Initialize {self.__class__.__name__}")
        self._items: dict[str, T] = {}
        for base in reversed(self.__class__.__mro__):
            for name, value in base.__dict__.items():
                if self._condition(value, self.item_type):
                    item = copy.deepcopy(value, memo={})
                    self._items[name] = item
                    setattr(self, name, item)

    def __iter__(self) -> Iterator[T]:
        return iter(self._items.values())

    def __getitem__(self, key: str) -> T:
        return self._items[key]

    def __call__(self, name: str) -> T:
        for item in self:
            if getattr(item, "name", None) == name:
                return item
        raise ValueError(f"{self.__class__.__name__} has no item with name '{name}'.")

    def items(self) -> Iterator[tuple[str, T]]:
        return iter(self._items.items())

    def keys(self) -> Iterable[str]:
        return self._items.keys()

    def values(self) -> Iterable[T]:
        return self._items.values()

    @staticmethod
    def _condition(value: Any, item_type: type) -> bool:
        return isinstance(value, item_type)


class EnumLikeClassContainer(EnumLikeContainer, Generic[T]):
    def __iter__(self) -> Iterator[Type[T]]:
        return EnumLikeContainer.__iter__(self)

    def __getitem__(self, key: str) -> Type[T]:
        return EnumLikeContainer.__getitem__(self, key)

    def __call__(self, name: str) -> Type[T]:
        return EnumLikeContainer.__call__(self, name)

    def items(self) -> Iterator[tuple[str, Type[T]]]:
        return EnumLikeContainer.items(self)

    def keys(self) -> Iterable[str]:
        return EnumLikeContainer.keys(self)

    def values(self) -> Iterable[Type[T]]:
        return EnumLikeContainer.values(self)

    @staticmethod
    def _condition(value: type, item_type: type) -> bool:
        return isinstance(value, type) and issubclass(value, item_type)
