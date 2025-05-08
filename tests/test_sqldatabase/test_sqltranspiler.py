import pprint
import sys
import textwrap
import unittest
from collections import OrderedDict
from collections.abc import Sequence
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parents[2]))

from sqldatabase import (
    ESqlDialect,
    SqlCreateTableStatement,
    SqlDatabase,
    SqlTranspiler,
)
from tests.test_sqldatabase.basetestcase import BaseTestCase
from tests.test_sqldatabase.usersdatabase import (
    UsersSqliteDatabase,
    UsersSqlServerDatabase,
)


class SqlTranspilerTestCase(BaseTestCase):
    def _print_test_data(
        self,
        sql: str,
        parameters: dict[str, Any] | Sequence | None,
        input_dialect: ESqlDialect | None,
        transpiled_sql: str,
        transpiled_parameters: dict[str, Any] | Sequence | None,
        output_dialect: ESqlDialect,
        expected_transpiled_sql: str | None,
        expected_transpiled_parameters: dict[str, Any] | Sequence | None,
    ):
        print("=" * 80)
        print(
            f"Transpile: {None if input_dialect is None else input_dialect.value} -> {output_dialect.value}"
        )
        print("=" * 80)
        print("SQL:")
        print("-" * 80)
        print(textwrap.indent(sql, "  "))
        print()
        print(
            textwrap.indent(
                f"parameters = {pprint.pformat(parameters, sort_dicts=False)}", "  "
            )
        )
        print("-" * 80)
        print("Transpiled SQL:")
        print("-" * 80)
        print(textwrap.indent(transpiled_sql, "  "))
        print()
        print(
            textwrap.indent(
                f"parameters = {pprint.pformat(transpiled_parameters, sort_dicts=False)}",
                "  ",
            )
        )
        print("-" * 80)
        print("Expected transpiled SQL:")
        print("-" * 80)
        print(textwrap.indent(str(expected_transpiled_sql), "  "))
        print()
        print(
            textwrap.indent(
                f"parameters = {pprint.pformat(expected_transpiled_parameters, sort_dicts=False)}",
                "  ",
            )
        )
        print()

    def _test_transpiled_sql(
        self,
        sql: str,
        parameters: dict[str, Any] | Sequence | None,
        input_dialect: ESqlDialect | None,
        transpiled_sql: str,
        transpiled_parameters: dict[str, Any] | Sequence | None,
        output_dialect: ESqlDialect,
        expected_transpiled_sql: str | None,
        expected_transpiled_parameters: dict[str, Any] | Sequence | None,
    ) -> None:
        self._print_test_data(
            sql,
            parameters,
            input_dialect,
            transpiled_sql,
            transpiled_parameters,
            output_dialect,
            expected_transpiled_sql,
            expected_transpiled_parameters,
        )
        if expected_transpiled_sql is not None:
            self.assertEqual(transpiled_sql, expected_transpiled_sql)
        if expected_transpiled_parameters is not None:
            if isinstance(transpiled_parameters, dict):
                transpiled_parameters = OrderedDict(transpiled_parameters)
            if isinstance(expected_transpiled_parameters, dict):
                expected_transpiled_parameters = OrderedDict(
                    expected_transpiled_parameters
                )
            self.assertEqual(transpiled_parameters, expected_transpiled_parameters)

    def _test_data(self, data_file_name: str) -> None:
        data = self._load_json_test_data(data_file_name)
        subtest_index = 0
        for (
            sql,
            parameters,
            input_dialect,
            expected_transpiled_sql,
            expected_transpiled_parameters,
            output_dialect,
        ) in data:
            input_dialect = ESqlDialect(input_dialect)
            output_dialect = ESqlDialect(output_dialect)
            if isinstance(expected_transpiled_parameters, list):
                expected_transpiled_parameters = tuple(expected_transpiled_parameters)
            with self.subTest(
                subtest_index=subtest_index,
                input_dialect=input_dialect.value,
                output_dialect=output_dialect.value,
            ):
                transpiled_sql, transpiled_parameters = SqlTranspiler(
                    output_dialect
                ).transpile(sql, parameters, input_dialect, pretty=True)
                self._test_transpiled_sql(
                    sql,
                    parameters,
                    input_dialect,
                    transpiled_sql,
                    transpiled_parameters,
                    output_dialect,
                    expected_transpiled_sql,
                    expected_transpiled_parameters,
                )
                subtest_index += 1

    def test_parse(self) -> None:
        transpiler = SqlTranspiler(ESqlDialect.SQLSERVER)
        sql = """
            INSERT INTO users (
              first_name,
              last_name
            )
            VALUES
              ('John', 'Doe')
            RETURNING id, email;
        """
        input_dialect = ESqlDialect.SQLITE
        parsed_sql = transpiler._parse(sql, input_dialect)
        self.assertIs(parsed_sql, transpiler._cache[(sql, input_dialect.value)])

    def test_sort_parameters(self) -> None:
        transpiler = SqlTranspiler(ESqlDialect.SQLSERVER)
        sql = """
            SELECT
              *
            FROM users
            WHERE
              users.name = :users_name
              AND users.age BETWEEN :users_age_lower AND :users_age_upper;
        """
        parameters = {
            "users_age_lower": 18,
            "users_age_upper": 65,
            "users_name": "John",
        }
        sorted_parameters = transpiler._sort_parameters(sql, parameters)
        self.assertEqual(
            OrderedDict(sorted_parameters),
            OrderedDict(
                {"users_name": "John", "users_age_lower": 18, "users_age_upper": 65}
            ),
        )

    def test_update_returning_and_output_clause(self) -> None:
        self._test_data("returning_and_output_clause.json")

    def test_update_named_parameters_and_positional_placeholders(self) -> None:
        self._test_data("named_parameters_and_positional_placeholders.json")

    def test_update_parameters(self) -> None:
        self._test_data("transpiled_parameters.json")

    def test_create_table_statement(self) -> None:
        databases: list[SqlDatabase] = [
            UsersSqliteDatabase(self.get_temp_dir_path() / "test_users.db"),
            UsersSqlServerDatabase("localhost", "test_users", trusted_connection=True),
        ]
        data = self._load_json_test_data("create_table_statement.json")
        for index, database in enumerate(databases):
            expected_transpiled_sql = data[index][0]
            output_dialect = ESqlDialect(data[index][1])

            self.assertEqual(database.dialect, output_dialect)

            statement = SqlCreateTableStatement(output_dialect, database.tables.USERS)
            self._test_transpiled_sql(
                statement._template_sql,
                statement._template_parameters,
                statement._template_dialect,
                statement.sql,
                statement.parameters,
                statement.dialect,
                expected_transpiled_sql,
                None,
            )
            database.close()


if __name__ == "__main__":
    unittest.main()
