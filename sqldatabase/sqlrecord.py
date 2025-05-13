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
    """
    Represents a record in a SQL table.

    Attributes:
        _data (dict[SqlColumn | SqlAggregateFunction, Any]): The data stored in the record.
    """
    def __init__(
        self, data: dict[SqlColumn | SqlAggregateFunction, Any] | None = None
    ) -> None:
        """
        Initialize a SqlRecord instance.

        Args:
            data (dict[SqlColumn | SqlAggregateFunction, Any] | None, optional): The data for the record. Defaults to None.
        """
        self._data: dict[SqlColumn | SqlAggregateFunction, Any] = {}

        if data is not None:
            for key, value in data.items():
                self[key] = value

    def __eq__(self, other: Any) -> bool:
        """
        Check if two SqlRecord instances are equal.

        Args:
            other (Any): The other instance to compare with.

        Returns:
            bool: True if the instances are equal, False otherwise.
        """
        return isinstance(other, SqlRecord) and self._data == other._data

    def __getitem__(self, key: SqlColumn | SqlAggregateFunction | int) -> Any:
        """
        Get the value associated with a key.

        Args:
            key (SqlColumn | SqlAggregateFunction | int): The key to retrieve the value for.

        Returns:
            Any: The value associated with the key.
        """
        resolved_key = self._resolve_key(key)
        return self._data[resolved_key]

    def __setitem__(
        self, key: SqlColumn | SqlAggregateFunction | int, value: Any
    ) -> None:
        """
        Set the value for a key.

        Args:
            key (SqlColumn | SqlAggregateFunction | int): The key to set the value for.
            value (Any): The value to set.
        """
        resolved_key = self._resolve_key(key)
        self._data[resolved_key] = value

    def __delitem__(self, key: SqlColumn | SqlAggregateFunction | int) -> None:
        """
        Delete the value associated with a key.

        Args:
            key (SqlColumn | SqlAggregateFunction | int): The key to delete the value for.
        """
        resolved_key = self._resolve_key(key)
        del self._data[resolved_key]

    def __iter__(self) -> Iterator:
        """
        Return an iterator over the keys of the record.

        Returns:
            Iterator: An iterator over the keys of the record.
        """
        return iter(self._data)

    def __len__(self) -> int:
        """
        Return the number of items in the record.

        Returns:
            int: The number of items in the record.
        """
        return len(self._data)

    def __contains__(self, key: Any) -> bool:
        """
        Check if a key is in the record.

        Args:
            key (Any): The key to check.

        Returns:
            bool: True if the key is in the record, False otherwise.
        """
        self._validate_key(key)
        return key in self._data

    def _validate_key(self, key: Any) -> None:
        """
        Validate the key type.

        Args:
            key (Any): The key to validate.

        Raises:
            TypeError: If the key type is invalid.
        """
        if not isinstance(key, (SqlColumn, SqlAggregateFunction)):
            raise TypeError(
                f"Invalid key type: {type(key)}."
                " Only SqlColumn or SqlFunction are allowed as SqlRecord keys."
            )

    def _resolve_key(self, key: Any) -> SqlColumn | SqlAggregateFunction:
        """
        Resolve the key to a SqlColumn or SqlAggregateFunction.

        Args:
            key (Any): The key to resolve.

        Returns:
            SqlColumn | SqlAggregateFunction: The resolved key.
        """
        if isinstance(key, int):
            return list(self._data.keys())[key]
        self._validate_key(key)
        return key

    def keys(self) -> KeysView[SqlColumn | SqlAggregateFunction]:
        """
        Return a view of the keys in the record.

        Returns:
            KeysView[SqlColumn | SqlAggregateFunction]: A view of the keys in the record.
        """
        return self._data.keys()

    def values(self) -> ValuesView[Any]:
        """
        Return a view of the values in the record.

        Returns:
            ValuesView[Any]: A view of the values in the record.
        """
        return self._data.values()

    def items(self) -> ItemsView[SqlColumn | SqlAggregateFunction, Any]:
        """
        Return a view of the items in the record.

        Returns:
            ItemsView[SqlColumn | SqlAggregateFunction, Any]: A view of the items in the record.
        """
        return self._data.items()

    @staticmethod
    def _parse_alias(
        alias: str,
    ) -> tuple[str | None, str | None, str | None]:
        """
        Parse an alias into its components.

        Args:
            alias (str): The alias to parse.
            database (SqlDatabase): The database instance.

        Returns:
            tuple[str | None, str | None, str | None]: The function name, table fully qualified name, and column name.
        """
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
        """
        Get an item by its alias.

        Args:
            alias (str): The alias of the item.
            database (SqlDatabase): The database instance.

        Returns:
            SqlColumn | SqlAggregateFunction: The item associated with the alias.
        """
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
        """
        Convert a value to its database representation.

        Args:
            item (SqlColumn | SqlAggregateFunction): The item associated with the value.
            value (Any): The value to convert.

        Returns:
            Any: The database representation of the value.
        """
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
        """
        Convert a value from its database representation.

        Args:
            item (SqlColumn | SqlAggregateFunction): The item associated with the value.
            value (Any): The database representation of the value.

        Returns:
            Any: The original value.
        """
        if (
            item.data_type is not None
            and item.data_type.from_database_converter is not None
        ):
            value = item.data_type.from_database_converter(value)
        if item.from_database_converter is not None:
            value = item.from_database_converter(value)
        return value

    def to_database_parameters(self) -> dict[str, Any]:
        """
        Convert the record to a dictionary of database parameters.

        Returns:
            dict[str, Any]: The dictionary of database parameters.
        """
        parameters = {}
        for item, value in self.items():
            parameter = item.generate_parameter_name()
            parameters[parameter] = self.to_database_value(item, value)
        return parameters

    @classmethod
    def from_database_row(
        cls, aliases: list[str], row: tuple | pyodbc.Row, database: SqlDatabase
    ) -> SqlRecord:
        """
        Create a SqlRecord instance from a database row.

        Args:
            aliases (list[str]): The list of aliases for the row.
            row (tuple | pyodbc.Row): The database row.
            database (SqlDatabase): The database instance.

        Returns:
            SqlRecord: The created SqlRecord instance.
        """
        record = cls()
        for alias, value in zip(aliases, row):
            item = cls._get_item_by_alias(alias, database)
            record[item] = cls.from_database_value(item, value)
        return record

    @staticmethod
    def to_json_value(item: SqlColumn | SqlAggregateFunction, value: Any) -> Any:
        """
        Convert a value to its JSON representation.

        Args:
            item (SqlColumn | SqlAggregateFunction): The item associated with the value.
            value (Any): The value to convert.

        Returns:
            Any: The JSON representation of the value.
        """
        if item.to_database_converter is not None:
            value = item.to_database_converter(value)
        if isinstance(value, bytes):
            value = base64.b64encode(value).decode("ascii")
        elif isinstance(value, (datetime.date, datetime.datetime, datetime.time)):
            value = value.isoformat()
        return value

    @staticmethod
    def from_json_value(item: SqlColumn | SqlAggregateFunction, value: Any) -> Any:
        """
        Convert a value from its JSON representation.

        Args:
            item (SqlColumn | SqlAggregateFunction): The item associated with the value.
            value (Any): The JSON representation of the value.

        Returns:
            Any: The original value.
        """
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
        """
        Convert the record to a JSON-serializable dictionary.

        Returns:
            dict[str, Any]: The JSON-serializable dictionary.
        """
        data = {}
        for item, value in self.items():
            data[item.alias] = self.to_json_value(item, value)
        return data

    @classmethod
    def from_json(cls, data: dict[str, Any], database: SqlDatabase) -> SqlRecord:
        """
        Create a SqlRecord instance from a JSON-serializable dictionary.

        Args:
            data (dict[str, Any]): The JSON-serializable dictionary.
            database (SqlDatabase): The database instance.

        Returns:
            SqlRecord: The created SqlRecord instance.
        """
        record = cls()
        for alias, value in data.items():
            item = cls._get_item_by_alias(alias, database)
            record[item] = cls.from_json_value(item, value)
        return record
