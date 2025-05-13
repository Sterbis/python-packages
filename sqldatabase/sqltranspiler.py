import copy
import re
from enum import Enum
from collections.abc import Sequence
from typing import Any, overload

import sqlglot
import sqlglot.expressions


class ESqlDialect(Enum):
    MYSQL = "mysql"
    POSTGRESQL = "postgres"
    SQLITE = "sqlite"
    SQLSERVER = "tsql"


class SqlTranspiler:
    """
    Transpiles SQL queries between different SQL dialects.

    Attributes:
        output_dialect (ESqlDialect): The target SQL dialect for transpilation.
        _cache (dict[tuple[str, str | None], sqlglot.Expression]): Cache for parsed SQL expressions.
    """
    _cache: dict[tuple[str, str | None], sqlglot.Expression] = {}

    def __init__(self, output_dialect: ESqlDialect) -> None:
        """
        Initialize a SqlTranspiler instance.

        Args:
            output_dialect (ESqlDialect): The target SQL dialect for transpilation.
        """
        self.output_dialect = output_dialect

    def transpile(
        self,
        sql: str,
        parameters: dict[str, Any] | Sequence | None = None,
        input_dialect: ESqlDialect | None = None,
        pretty: bool = False,
    ) -> tuple[str, dict[str, Any] | Sequence]:
        """
        Transpile a SQL query and its parameters to the target dialect.

        Args:
            sql (str): The SQL query to transpile.
            parameters (dict[str, Any] | Sequence | None, optional): Parameters for the query. Defaults to None.
            input_dialect (ESqlDialect | None, optional): The source SQL dialect. Defaults to None.
            pretty (bool, optional): Whether to format the SQL query. Defaults to False.

        Returns:
            tuple[str, dict[str, Any] | Sequence]: The transpiled SQL query and its parameters.
        """
        transpiled_sql = self.transpile_sql(sql, input_dialect, pretty)
        parameters = self.transpile_parameters(sql, parameters)
        return transpiled_sql, parameters

    def transpile_sql(
        self,
        sql: str,
        input_dialect: ESqlDialect | None = None,
        pretty: bool = False,
    ) -> str:
        """
        Transpile a SQL query to the target dialect.

        Args:
            sql (str): The SQL query to transpile.
            input_dialect (ESqlDialect | None, optional): The source SQL dialect. Defaults to None.
            pretty (bool, optional): Whether to format the SQL query. Defaults to False.

        Returns:
            str: The transpiled SQL query.
        """
        parsed_sql = self._parse(sql, input_dialect)
        parsed_sql = self._update_parsed_sql(parsed_sql)
        transpiled_sql = parsed_sql.sql(
            dialect=self.output_dialect.value, pretty=pretty
        )
        transpiled_sql = self._update_transpiled_sql(transpiled_sql)
        return transpiled_sql

    def transpile_parameters(
        self,
        sql: str,
        parameters: dict[str, Any] | Sequence | None,
    ) -> dict[str, Any] | Sequence:
        """
        Transpile the parameters of a SQL query to the target dialect.

        Args:
            sql (str): The SQL query.
            parameters (dict[str, Any] | Sequence | None): Parameters for the query.
            input_dialect (ESqlDialect | None): The source SQL dialect.

        Returns:
            dict[str, Any] | Sequence: The transpiled parameters.
        """
        if parameters is None:
            parameters = ()
        else:
            parameters = self._sort_parameters(sql, parameters)
        if isinstance(parameters, dict) and self.output_dialect in (
            ESqlDialect.SQLSERVER,
            ESqlDialect.POSTGRESQL,
            ESqlDialect.MYSQL,
        ):
            parameters = tuple(parameters.values())
        elif (
            isinstance(parameters, Sequence)
            and self.output_dialect == ESqlDialect.SQLITE
        ):
            parameters = {
                f"parameter_{index + 1}": parameter
                for index, parameter in enumerate(parameters)
            }
        return parameters

    def _parse(
        self, sql: str, input_dialect: ESqlDialect | None = None
    ) -> sqlglot.Expression:
        """
        Parse a SQL query using the specified input dialect.

        Args:
            sql (str): The SQL query to parse.
            input_dialect (ESqlDialect | None, optional): The source SQL dialect. Defaults to None.

        Returns:
            sqlglot.Expression: The parsed SQL expression.
        """
        dialect = (
            input_dialect.value
            if isinstance(input_dialect, ESqlDialect)
            else input_dialect
        )
        cache_key = (sql, dialect)
        if cache_key in self._cache:
            return self._cache[cache_key]
        parsed_sql = sqlglot.parse_one(sql, dialect=dialect)
        self._cache[cache_key] = parsed_sql
        return parsed_sql

    @staticmethod
    def _remove_string_literals(sql: str) -> str:
        """
        Remove string literals from a SQL query.

        Args:
            sql (str): The SQL query.

        Returns:
            str: The SQL query with string literals removed.
        """
        pattern = re.compile(
            r"('(?:''|[^'])*')"  # single-quoted strings
            r'|("(?:[^"]|"")*")'  # double-quoted strings (optionally used for identifiers or strings)
        )
        return pattern.sub("", sql)

    def _find_named_parameters(self, sql: str) -> list[str]:
        """
        Find named parameters in a SQL query.

        Args:
            sql (str): The SQL query.

        Returns:
            list[str]: A list of named parameters.
        """
        preprocessed_sql = self._remove_string_literals(sql)
        return re.findall(r"(?<!:)[:@$][a-zA-Z_][a-zA-Z0-9_]*", preprocessed_sql)

    def _find_positional_placeholders(self, sql: str) -> list[str]:
        """
        Find positional placeholders in a SQL query.

        Args:
            sql (str): The SQL query.

        Returns:
            list[str]: A list of positional placeholders.
        """
        preprocessed_sql = self._remove_string_literals(sql)
        return re.findall(r"[$@]\d+|\?", preprocessed_sql)

    def _find_named_parameters_and_positional_placeholders(self, sql: str) -> list[str]:
        """
        Find both named parameters and positional placeholders in a SQL query.

        Args:
            sql (str): The SQL query.

        Returns:
            list[str]: A list of named parameters and positional placeholders.
        """
        return self._find_named_parameters(sql) + self._find_positional_placeholders(
            sql
        )

    @staticmethod
    def _is_positional_placeholder(value: str) -> bool:
        """
        Check if a value is a positional placeholder.

        Args:
            value (str): The value to check.

        Returns:
            bool: True if the value is a positional placeholder, False otherwise.
        """
        return value == "?" or re.fullmatch(r"[$@]\d+", value) is not None

    @staticmethod
    def _is_named_parameter(value: str) -> bool:
        """
        Check if a value is a named parameter.

        Args:
            value (str): The value to check.

        Returns:
            bool: True if the value is a named parameter, False otherwise.
        """
        return re.fullmatch(r"[:@$][a-zA-Z_][a-zA-Z0-9_]*", value) is not None

    @overload
    def _sort_parameters(
        self, sql: str, parameters: dict[str, Any]
    ) -> dict[str, Any]: ...

    @overload
    def _sort_parameters(self, sql: str, parameters: Sequence) -> tuple: ...

    def _sort_parameters(
        self,
        sql: str,
        parameters: dict[str, Any] | Sequence,
    ) -> dict[str, Any] | tuple:
        """
        Sort the parameters of a SQL query.

        Args:
            sql (str): The SQL query.
            parameters (dict[str, Any] | Sequence): Parameters for the query.

        Returns:
            dict[str, Any] | tuple: The sorted parameters.
        """
        if isinstance(parameters, dict):
            parameters = {
                parameter.lstrip(":@$"): parameters[parameter.lstrip(":@$")]
                for parameter in self._find_named_parameters(sql)
            }
        elif isinstance(parameters, Sequence):
            placeholders = self._find_positional_placeholders(sql)
            indexes = [
                int(placeholder.lstrip("$")) - 1
                for placeholder in placeholders
                if placeholder.startswith("$")
            ]
            if len(indexes) == len(parameters):
                parameters = tuple(parameters[index] for index in indexes)
            elif len(indexes) == 0:
                parameters = tuple(parameters)
            else:
                assert (
                    False
                ), f"Unexpected positional placeholders found in sql:\n{sql}\n\nplaceholders = {placeholders}"
        return parameters

    def _update_parsed_sql(self, parsed_sql: sqlglot.Expression) -> sqlglot.Expression:
        """
        Update the parsed SQL expression.

        Args:
            parsed_sql (sqlglot.Expression): The parsed SQL expression.

        Returns:
            sqlglot.Expression: The updated SQL expression.
        """
        parsed_sql = copy.deepcopy(parsed_sql)
        self._update_returning_and_output_clause(parsed_sql)
        return parsed_sql

    def _update_returning_and_output_clause(
        self, parsed_sql: sqlglot.Expression
    ) -> None:
        """
        Update the RETURNING and OUTPUT clauses in the parsed SQL expression.

        Args:
            parsed_sql (sqlglot.Expression): The parsed SQL expression.
        """
        returning = parsed_sql.find(sqlglot.expressions.Returning)
        if returning is not None:
            if self.output_dialect == ESqlDialect.MYSQL:
                returning.pop()
            else:
                if isinstance(
                    parsed_sql, (sqlglot.expressions.Insert, sqlglot.expressions.Update)
                ):
                    virtual_table_name = "INSERTED"
                elif isinstance(parsed_sql, sqlglot.expressions.Delete):
                    virtual_table_name = "DELETED"
                else:
                    assert (
                        False
                    ), f"Unexpected statement with returning clause: {repr(parsed_sql)}"
                for column in returning.find_all(sqlglot.expressions.Column):
                    table_name: str | None = column.table or None
                    if self.output_dialect == ESqlDialect.SQLSERVER:
                        column.set("table", virtual_table_name)
                    elif self.output_dialect in (
                        ESqlDialect.SQLITE,
                        ESqlDialect.POSTGRESQL,
                    ):
                        if table_name is not None:
                            if table_name.upper() == virtual_table_name:
                                column.set("table", None)
                            elif table_name.upper().startswith(
                                f"{virtual_table_name}."
                            ):
                                column.set(
                                    "table", table_name[len(virtual_table_name) + 1 :]
                                )

    def _update_transpiled_sql(
        self, sql: str,
    ) -> str:
        """
        Update the transpiled SQL query.

        Args:
            sql (str): The transpiled SQL query.
            input_dialect (ESqlDialect | None, optional): The source SQL dialect. Defaults to None.

        Returns:
            str: The updated SQL query.
        """
        sql = self._update_named_parameters_and_positional_placeholders(sql)
        sql = self._update_output_clause(sql)
        return sql

    def _update_named_parameters_and_positional_placeholders(self, sql: str) -> str:
        """
        Update named parameters and positional placeholders in a SQL query.

        Args:
            sql (str): The SQL query.

        Returns:
            str: The updated SQL query.
        """
        parameters_and_placeholders = (
            self._find_named_parameters_and_positional_placeholders(sql)
        )
        search_location = 0
        for index, parameter_or_placeholder in enumerate(parameters_and_placeholders):
            location = sql.find(parameter_or_placeholder, search_location)
            if self.output_dialect == ESqlDialect.SQLITE:
                if self._is_positional_placeholder(parameter_or_placeholder):
                    replacement = f":parameter_{index + 1}"
                elif self._is_named_parameter(parameter_or_placeholder):
                    replacement = f":{parameter_or_placeholder.lstrip(":@$")}"
                else:
                    assert (
                        False
                    ), f"Unexpected parameter or placeholder: {parameter_or_placeholder}"
            elif self.output_dialect == ESqlDialect.POSTGRESQL:
                replacement = f"${index + 1}"
            elif self.output_dialect in (ESqlDialect.SQLSERVER, ESqlDialect.MYSQL):
                replacement = "?"
            else:
                assert False, f"Unexpected output dialect: {self.output_dialect}"
            sql = sql[:location] + sql[location:].replace(
                parameter_or_placeholder, replacement, 1
            )
            search_location = location + len(replacement)
        return sql

    def _update_output_clause(self, sql: str) -> str:
        """
        Update the OUTPUT clause in a SQL query.

        Args:
            sql (str): The SQL query.

        Returns:
            str: The updated SQL query.
        """
        if self.output_dialect == ESqlDialect.SQLSERVER:
            pattern = r"DELETE\s(?P<output_clause>\bOUTPUT\b.*?)(?P<from_clause>\bFROM\b.*?)(?=\bWHERE\b|$)"
            match = re.search(pattern, sql, flags=re.DOTALL)
            if match:
                output_clause = match.group("output_clause")
                from_clause = match.group("from_clause")
                sql = re.sub(
                    pattern,
                    f"DELETE {from_clause.strip()}\n{output_clause.strip()}\n",
                    sql,
                    flags=re.DOTALL,
                )
        return sql
