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
    _cache: dict[tuple[str, str | None], sqlglot.Expression] = {}

    def __init__(self, output_dialect: ESqlDialect) -> None:
        self.output_dialect = output_dialect

    def transpile(
        self,
        sql: str,
        parameters: dict[str, Any] | Sequence | None = None,
        input_dialect: ESqlDialect | None = None,
        pretty: bool = False,
    ) -> tuple[str, dict[str, Any] | Sequence]:
        transpiled_sql = self.transpile_sql(sql, input_dialect, pretty)
        parameters = self.transpile_parameters(sql, parameters, input_dialect)
        return transpiled_sql, parameters

    def transpile_sql(
        self,
        sql: str,
        input_dialect: ESqlDialect | None = None,
        pretty: bool = False,
    ) -> str:
        parsed_sql = self._parse(sql, input_dialect)
        parsed_sql = self._update_parsed_sql(parsed_sql)
        transpiled_sql = parsed_sql.sql(
            dialect=self.output_dialect.value, pretty=pretty
        )
        transpiled_sql = self._update_transpiled_sql(transpiled_sql, input_dialect)
        return transpiled_sql

    def transpile_parameters(
        self,
        sql: str,
        parameters: dict[str, Any] | Sequence | None,
        input_dialect: ESqlDialect | None,
    ) -> dict[str, Any] | Sequence:
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
        pattern = re.compile(
            r"('(?:''|[^'])*')"  # single-quoted strings
            r'|("(?:[^"]|"")*")'  # double-quoted strings (optionally used for identifiers or strings)
        )
        return pattern.sub("", sql)

    def _find_named_parameters(self, sql: str) -> list[str]:
        preprocessed_sql = self._remove_string_literals(sql)
        return re.findall(r"(?<!:)[:@$][a-zA-Z_][a-zA-Z0-9_]*", preprocessed_sql)

    def _find_positional_placeholders(self, sql: str) -> list[str]:
        preprocessed_sql = self._remove_string_literals(sql)
        return re.findall(r"[$@]\d+|\?", preprocessed_sql)

    def _find_named_parameters_and_positional_placeholders(self, sql: str) -> list[str]:
        return self._find_named_parameters(sql) + self._find_positional_placeholders(
            sql
        )

    @staticmethod
    def _is_positional_placeholder(value: str) -> bool:
        return value == "?" or re.fullmatch(r"[$@]\d+", value) is not None

    @staticmethod
    def _is_named_parameter(value: str) -> bool:
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
        parsed_sql = copy.deepcopy(parsed_sql)
        self._update_returning_and_output_clause(parsed_sql)
        return parsed_sql

    def _update_returning_and_output_clause(
        self, parsed_sql: sqlglot.Expression
    ) -> None:
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
        self, sql: str, input_dialect: ESqlDialect | None
    ) -> str:
        sql = self._update_named_parameters_and_positional_placeholders(sql)
        sql = self._update_output_clause(sql)
        return sql

    def _update_named_parameters_and_positional_placeholders(self, sql: str) -> str:
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
