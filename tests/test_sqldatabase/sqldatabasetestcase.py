import datetime
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parents[2]))

from sqldatabase import (
    SqlColumn,
    SqlRecord,
    SqlTable,
    SqlDatabase,
)
from tests.test_sqldatabase.basetestcase import BaseTestCase
from tests.test_sqldatabase.dictionarydatabase import (
    DictionaryDatabaseTables,
    EPartOfSpeech,
    ETag,
)


class SqlDatabaseTestCase(BaseTestCase):
    database: SqlDatabase[DictionaryDatabaseTables]

    @classmethod
    def _load_test_dictionary(cls) -> dict:
        dictionary = cls._load_json_test_data("dictionary.json")

        for table_name, records in dictionary.items():
            if table_name in ("meanings", "tags", "user_progress"):
                for record in records:
                    if table_name == "meanings":
                        record["part_of_speech"] = EPartOfSpeech(
                            record["part_of_speech"]
                        )
                    elif table_name == "tags":
                        record["tag"] = ETag(record["tag"])
                    elif table_name == "user_progress":
                        record["last_seen"] = datetime.date.fromisoformat(
                            record["last_seen"]
                        )

        return dictionary

    @classmethod
    def _insert_test_dictionary(cls, dictionary: dict) -> None:
        for table_name, rows in dictionary.items():
            table = cls.database.tables(table_name)
            record = SqlRecord()
            for row in rows:
                for column_name, value in row.items():
                    column: SqlColumn = table.columns(column_name)
                    if column.converter:
                        value = column.converter(value)
                    record[column] = value
                table.insert_records(record)
        cls.database.commit()

    def _test_foreign_key_column_to_primary_key_column_reference(self) -> None:
        tables = self.database.tables
        columns = [
            (tables.MEANIGS.columns.WORD_ID, tables.WORDS.columns.ID),
            (tables.EXAMPLES.columns.MEANING_ID, tables.MEANIGS.columns.ID),
            (tables.MEANING_TAGS.columns.MEANING_ID, tables.MEANIGS.columns.ID),
            (tables.MEANING_TAGS.columns.TAG_ID, tables.TAGS.columns.ID),
            (tables.USER_PROGRESS.columns.USER_ID, tables.USERS.columns.ID),
            (tables.USER_PROGRESS.columns.MEANING_ID, tables.MEANIGS.columns.ID),
        ]
        for foreign_key_column, referenced_column in columns:
            with self.subTest(
                foreign_key_column=foreign_key_column.fully_qualified_name,
                referenced_column=referenced_column.fully_qualified_name,
            ):
                self.assertIs(foreign_key_column.reference, referenced_column)
                self.assertIn(foreign_key_column, referenced_column._foreign_keys)

    def _test_column_to_table_reference(self) -> None:
        for table in self.database.tables:
            for column in table.columns:
                with self.subTest(
                    column=column.fully_qualified_name, table=table.fully_qualified_name
                ):
                    self.assertIs(column.table, table)

    def _test_table_to_database_reference(self) -> None:
        database = self.database
        for table in database.tables:
            with self.subTest(table=table.fully_qualified_name, database=database.name):
                self.assertIs(table.database, database)

    def _test_data_type_to_database_reference(self) -> None:
        database = self.database
        for table in database.tables:
            for column in table.columns:
                with self.subTest(
                    column=column.fully_qualified_name, database=database.name
                ):
                    self.assertIs(column.data_type.database, database)

    def _test_data_type_converters(self) -> None:
        tables = self.database.tables
        data: list[tuple[SqlTable, SqlColumn, type]] = [
            (tables.MEANIGS, tables.MEANIGS.columns.PART_OF_SPEECH, EPartOfSpeech),
            (tables.TAGS, tables.TAGS.columns.TAG, ETag),
            (
                tables.USER_PROGRESS,
                tables.USER_PROGRESS.columns.LAST_SEEN,
                datetime.date,
            ),
        ]
        for table, column, data_type in data:
            with self.subTest(
                table=table.name,
                column=column.name,
                data_type=data_type,
            ):
                records = table.select_records(column, limit=1)
                value = records[0][column]
                self.assertIsInstance(value, data_type)

    def _test_words_table_record_count(self) -> None:
        table = self.database.tables.WORDS
        count = table.record_count()
        self.assertEqual(count, 3)
