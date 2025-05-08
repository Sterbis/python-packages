import copy
import re
from enum import Enum
from collections.abc import Sequence
from typing import Any

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
        if input_dialect != self.output_dialect:
            parsed_sql = self._update_parsed_sql(parsed_sql)
        transpiled_sql = parsed_sql.sql(
            dialect=self.output_dialect.value, pretty=pretty
        )
        if input_dialect != self.output_dialect:
            transpiled_sql = self._update_transpiled_sql(transpiled_sql, input_dialect)
        return transpiled_sql

    def transpile_parameters(
        self,
        sql: str,
        parameters: dict[str, Any] | Sequence | None,
        input_dialect: ESqlDialect | None,
    ) -> dict[str, Any] | Sequence:
        if isinstance(parameters, dict):
            parameters = self._sort_parameters(sql, parameters)
            if self.output_dialect in (ESqlDialect.POSTGRESQL, ESqlDialect.MYSQL):
                parameters = tuple(parameters.values())
        elif isinstance(parameters, Sequence):
            parameters = tuple(parameters)
            if (
                input_dialect == ESqlDialect.POSTGRESQL
                and self.output_dialect != ESqlDialect.POSTGRESQL
            ):
                placeholders = self._find_named_parameters_and_positional_placeholders(
                    sql
                )
                indexes = [
                    int(placeholder.lstrip("$")) - 1 for placeholder in placeholders
                ]
                parameters = tuple(parameters[index] for index in indexes)
            if self.output_dialect in (ESqlDialect.SQLITE, ESqlDialect.SQLSERVER):
                parameters = {
                    f"parameter_{index + 1}": parameter
                    for index, parameter in enumerate(parameters)
                }
        elif parameters is None:
            parameters = ()
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

    def _find_named_parameters_and_positional_placeholders(self, sql: str) -> list[str]:
        parameters = re.findall(r"(?<!:):\b\w+\b|@\b\w+\b|\$\b\w+\b", sql)
        regex = re.compile(
            r"""
            (                             # Group 1: string literals
                (?:'[^']*')               # single-quoted string
                |(?:\"[^\"]*\")           # double-quoted string
            )
            |
            (\?)                          # Group 2: bare question mark
        """,
            re.VERBOSE,
        )
        parameters += [
            match.group(2) for match in regex.finditer(sql) if match.group(2)
        ]
        return parameters

    def _sort_parameters(
        self,
        sql: str,
        parameters: dict[str, object],
    ) -> dict[str, object]:
        return {
            parameter.lstrip(":@$"): parameters[parameter.lstrip(":@$")]
            for parameter in self._find_named_parameters_and_positional_placeholders(
                sql
            )
        }

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
        sql = self._update_named_parameters_and_positional_placeholders(
            sql, input_dialect
        )
        return sql

    def _update_named_parameters_and_positional_placeholders(
        self, sql: str, input_dialect: ESqlDialect | None
    ) -> str:
        parameters = self._find_named_parameters_and_positional_placeholders(sql)
        for index, parameter in enumerate(parameters):
            if self.output_dialect in (ESqlDialect.SQLITE, ESqlDialect.SQLSERVER):
                if self.output_dialect == ESqlDialect.SQLITE:
                    symbol = ":"
                elif self.output_dialect == ESqlDialect.SQLSERVER:
                    symbol = "@"
                else:
                    assert False, f"Unexpected output dialect: {self.output_dialect}"
                if input_dialect in (ESqlDialect.POSTGRESQL, ESqlDialect.MYSQL):
                    sql = sql.replace(parameter, f"{symbol}parameter_{index + 1}", 1)
                else:
                    sql = sql.replace(parameter, f"{symbol}{parameter.lstrip(":@")}", 1)
            elif self.output_dialect == ESqlDialect.POSTGRESQL:
                sql = sql.replace(parameter, f"${index + 1}", 1)
            elif self.output_dialect == ESqlDialect.MYSQL:
                sql = sql.replace(parameter, "?", 1)
        return sql
