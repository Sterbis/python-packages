import copy
from collections.abc import ItemsView, KeysView, ValuesView
from typing import TypeVar, Generic, Type, Iterator, Any


T = TypeVar("T")


class EnumLikeContainer(Generic[T]):
    item_type: Type[T]

    def __init__(self) -> None:
        self._items: dict[str, T] = {}
        for base in reversed(self.__class__.__mro__):
            for name, value in base.__dict__.items():
                if self._condition(value, self.item_type):
                    item = copy.deepcopy(value, memo={})
                    self._items[name] = item
                    setattr(self, name, item)

    def __getitem__(self, key: str) -> T:
        return self._items[key]

    def __iter__(self) -> Iterator[T]:
        return iter(self._items.values())

    def __len__(self) -> int:
        return len(self._items)

    def __contains__(self, key: Any) -> bool:
        return key in self._items.values()

    def __call__(self, name: str) -> T:
        for item in self:
            if getattr(item, "name", None) == name:
                return item
        raise ValueError(f"{self.__class__.__name__} has no item with name '{name}'.")

    def keys(self) -> KeysView[str]:
        return self._items.keys()

    def values(self) -> ValuesView[T]:
        return self._items.values()

    def items(self) -> ItemsView[str, T]:
        return self._items.items()

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

    def keys(self) -> KeysView[str]:
        return EnumLikeContainer.keys(self)

    def values(self) -> ValuesView[Type[T]]:
        return EnumLikeContainer.values(self)

    def items(self) -> ItemsView[str, Type[T]]:
        return EnumLikeContainer.items(self)

    @staticmethod
    def _condition(value: type, item_type: type) -> bool:
        return isinstance(value, type) and issubclass(value, item_type)


class EnumLikeMixedContainer(EnumLikeContainer, Generic[T]):
    def __iter__(self) -> Iterator[T | Type[T]]:
        return EnumLikeContainer.__iter__(self)

    def __getitem__(self, key: str) -> T | Type[T]:
        return EnumLikeContainer.__getitem__(self, key)

    def __call__(self, name: str) -> T | Type[T]:
        return EnumLikeContainer.__call__(self, name)

    def keys(self) -> KeysView[str]:
        return EnumLikeContainer.keys(self)

    def values(self) -> ValuesView[T | Type[T]]:
        return EnumLikeContainer.values(self)

    def items(self) -> ItemsView[str, T | Type[T]]:
        return EnumLikeContainer.items(self)

    @staticmethod
    def _condition(value: type, item_type: type) -> bool:
        return isinstance(value, item_type) or (
            isinstance(value, type) and issubclass(value, item_type)
        )
