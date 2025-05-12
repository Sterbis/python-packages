import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from tests.test_sqldatabase.dictionarydatabase import (
    DictionarySqlServerDatabase,
)
from tests.test_sqldatabase.sqldatabasetestcase import SqlDatabaseTestCase


class SqlServerDatabaseTestCase(SqlDatabaseTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.database = DictionarySqlServerDatabase(
            server="localhost",
            database="test_dictionary",
            trusted_connection=True,
        )
        cls.setup_database()

    def test_connection(self) -> None:
        pass

    def test_foreign_key_column_to_primary_key_column_reference(self) -> None:
        self._test_foreign_key_column_to_primary_key_column_reference()

    def test_column_to_table_reference(self) -> None:
        self._test_column_to_table_reference()

    def test_table_to_database_reference(self) -> None:
        self._test_table_to_database_reference()

    def test_data_type_to_database_reference(self) -> None:
        self._test_data_type_to_database_reference()

    def test_data_type_converters(self) -> None:
        self._test_data_type_converters()

    def test_words_table_record_count(self) -> None:
        self._test_words_table_record_count()

    def test_select_words_table_records(self) -> None:
        self._test_select_words_table_records()

    def test_select_word_meanings(self) -> None:
        self._test_select_word_meanings()

    def test_select_word_definitions(self) -> None:
        self._test_select_word_definitions()

    def test_select_word_examples(self) -> None:
        self._test_select_word_examples()

    def test_select_words_tags(self) -> None:
        self._test_select_words_tags()

    def test_select_meanings_never_seen_by_user(self) -> None:
        self._test_select_meanings_never_seen_by_user()

    def test_select_users_ordered_by_correct_answers(self) -> None:
        self._test_select_users_ordered_by_correct_answers()

    def test_select_word_meanings_count(self) -> None:
        self._test_select_word_meanings_count()

    def test_select_user_answers_accuracy(self) -> None:
        self._test_select_user_answers_accuracy()

    def test_insert_word_entry(self) -> None:
        self._test_insert_word_entry()

    def test_update_correct_answers_count(self) -> None:
        self._test_update_correct_answers_count()

    def test_delete_user_and_user_progress(self) -> None:
        self._test_delete_user_and_user_progress()


if __name__ == "__main__":
    unittest.main()
