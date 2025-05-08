from collections.abc import ItemsView, KeysView, MutableMapping, ValuesView
from typing import Any, Iterator


from .sqlcolumn import SqlColumn
from .sqlfunction import SqlAggregateFunction


class SqlRecord(MutableMapping):
    def __init__(
        self, data: dict[SqlColumn | SqlAggregateFunction, Any] | None = None
    ) -> None:
        self._data: dict[SqlColumn | SqlAggregateFunction, Any] = {}

        if data is not None:
            for key, value in data.items():
                self[key] = value

    def _validate_key(self, key: Any) -> None:
        if not isinstance(key, (SqlColumn, SqlAggregateFunction)):
            raise TypeError(
                f"Invalid key type: {type(key)}."
                " Only SqlColumn or SqlFunction are allowed as SqlRecord keys."
            )

    def _resolve_key(self, key: Any) -> SqlColumn | SqlAggregateFunction:
        if isinstance(key, int):
            return list(self._data.keys())[key]
        self._validate_key(key)
        return key

    def __getitem__(self, key: SqlColumn | SqlAggregateFunction | int) -> Any:
        resolved_key = self._resolve_key(key)
        return self._data[resolved_key]

    def __setitem__(
        self, key: SqlColumn | SqlAggregateFunction | int, value: Any
    ) -> None:
        resolved_key = self._resolve_key(key)
        self._data[resolved_key] = value

    def __delitem__(self, key: SqlColumn | SqlAggregateFunction | int) -> None:
        resolved_key = self._resolve_key(key)
        del self._data[resolved_key]

    def __iter__(self) -> Iterator:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __contains__(self, key: Any) -> bool:
        self._validate_key(key)
        return key in self._data

    def keys(self) -> KeysView[SqlColumn | SqlAggregateFunction]:
        return self._data.keys()

    def values(self) -> ValuesView[Any]:
        return self._data.values()

    def items(self) -> ItemsView[SqlColumn | SqlAggregateFunction, Any]:
        return self._data.items()

    def generate_parameters(self) -> dict[str, Any]:
        parameters = {}
        for item, value in self.items():
            parameter = item.generate_parameter_name()
            if item.adapter is not None:
                value = item.adapter(value)
            if item.data_type is not None and item.data_type.adapter is not None:
                value = item.data_type.adapter(value)
            parameters[parameter] = value
        return parameters
