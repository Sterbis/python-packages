from __future__ import annotations
import base64
import datetime
from collections.abc import ItemsView, KeysView, MutableMapping, ValuesView
from typing import TYPE_CHECKING, Any, Iterator

import pyodbc  # type: ignore

from .sqlcolumn import SqlColumn
from .sqlfunction import SqlAggregateFunction, SqlAggregateFunctionWithMandatoryColumn

if TYPE_CHECKING:
    from .sqldatabase import SqlDatabase


class SqlRecord(MutableMapping):
    def __init__(
        self, data: dict[SqlColumn | SqlAggregateFunction, Any] | None = None
    ) -> None:
        self._data: dict[SqlColumn | SqlAggregateFunction, Any] = {}

        if data is not None:
            for key, value in data.items():
                self[key] = value

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, SqlRecord) and self._data == other._data

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

    def keys(self) -> KeysView[SqlColumn | SqlAggregateFunction]:
        return self._data.keys()

    def values(self) -> ValuesView[Any]:
        return self._data.values()

    def items(self) -> ItemsView[SqlColumn | SqlAggregateFunction, Any]:
        return self._data.items()

    @staticmethod
    def _parse_alias(
        alias: str,
    ) -> tuple[str | None, str | None, str | None]:
        function_name = table_fully_qualified_name = column_name = None
        alias_parts = alias.split(".")
        if "FUNCTION" in alias_parts:
            function_identifier_index = alias_parts.index("FUNCTION")
            function_name = alias_parts[function_identifier_index + 1]
        if "COLUMN" in alias_parts:
            column_identifier_index = alias_parts.index("COLUMN")
            column_fully_qualified_name_parts = alias_parts[
                column_identifier_index + 1 :
            ]
            table_fully_qualified_name = ".".join(
                column_fully_qualified_name_parts[:-1]
            )
            column_name = column_fully_qualified_name_parts[-1]
        return function_name, table_fully_qualified_name, column_name

    @classmethod
    def _get_item_by_alias(
        cls, alias: str, database: SqlDatabase
    ) -> SqlColumn | SqlAggregateFunction:
        function_name, table_fully_qualified_name, column_name = cls._parse_alias(
            alias
        )
        if table_fully_qualified_name is not None and column_name is not None:
            column = database.get_table(table_fully_qualified_name).get_column(
                column_name
            )
        else:
            column = None
        if function_name is not None:
            function_class = database.functions(function_name)
            assert not (
                column is None
                and issubclass(function_class, SqlAggregateFunctionWithMandatoryColumn)
            ), f"Unexpected alias format: {alias}"
            item: SqlColumn | SqlAggregateFunction = function_class(column)
        elif column is not None:
            item = column
        else:
            assert False, f"Unexpected alias format: {alias}"
        return item

    @staticmethod
    def to_database_value(item: SqlColumn | SqlAggregateFunction, value: Any) -> Any:
        if item.to_database_converter is not None:
            value = item.to_database_converter(value)
        if (
            item.data_type is not None
            and item.data_type.to_database_converter is not None
        ):
            value = item.data_type.to_database_converter(value)
        return value

    @staticmethod
    def from_database_value(item: SqlColumn | SqlAggregateFunction, value: Any) -> Any:
        if (
            item.data_type is not None
            and item.data_type.from_database_converter is not None
        ):
            value = item.data_type.from_database_converter(value)
        if item.from_database_converter is not None:
            value = item.from_database_converter(value)
        return value

    def to_database_parameters(self) -> dict[str, Any]:
        parameters = {}
        for item, value in self.items():
            parameter = item.generate_parameter_name()
            parameters[parameter] = self.to_database_value(item, value)
        return parameters

    @classmethod
    def from_database_row(
        cls, aliases: list[str], row: tuple | pyodbc.Row, database: SqlDatabase
    ) -> SqlRecord:
        record = cls()
        for alias, value in zip(aliases, row):
            item = cls._get_item_by_alias(alias, database)
            record[item] = cls.from_database_value(item, value)
        return record

    @staticmethod
    def to_json_value(item: SqlColumn | SqlAggregateFunction, value: Any) -> Any:
        if item.to_database_converter is not None:
            value = item.to_database_converter(value)
        if isinstance(value, bytes):
            value = base64.b64encode(value).decode("ascii")
        elif isinstance(value, (datetime.date, datetime.datetime, datetime.time)):
            value = value.isoformat()
        return value

    @staticmethod
    def from_json_value(item: SqlColumn | SqlAggregateFunction, value: Any) -> Any:
        if item.data_type is not None:
            if item.data_type.type == bytes:
                value = base64.b64decode(value.encode("ascii"))
            elif item.data_type.type == datetime.date:
                value = datetime.date.fromisoformat(value)
            elif item.data_type.type == datetime.datetime:
                value = datetime.datetime.fromisoformat(value)
            elif item.data_type.type == datetime.time:
                value = datetime.time.fromisoformat(value)
        if item.from_database_converter is not None:
            value = item.from_database_converter(value)
        return value

    def to_json(self) -> dict[str, Any]:
        data = {}
        for item, value in self.items():
            data[item.alias] = self.to_json_value(item, value)
        return data

    @classmethod
    def from_json(cls, data: dict[str, Any], database: SqlDatabase) -> SqlRecord:
        record = cls()
        for alias, value in data.items():
            item = cls._get_item_by_alias(alias, database)
            record[item] = cls.from_json_value(item, value)
        return record
