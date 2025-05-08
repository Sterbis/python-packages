import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from tests.test_sqldatabase.dictionarydatabase import (
    DictionarySqliteDatabase,
)
from tests.test_sqldatabase.sqldatabasetestcase import SqlDatabaseTestCase


class SqliteDatabaseTestCase(SqlDatabaseTestCase):
    database: DictionarySqliteDatabase

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        path = cls.get_temp_dir_path() / "test_dictionary.db"
        cls.database = DictionarySqliteDatabase(path)  # type: ignore
        cls.database.create_all_tables(if_not_exists=True)
        records = cls.database.tables.WORDS.select_records()
        if len(records) == 0:
            dictionary = cls._load_test_dictionary()
            cls._insert_test_dictionary(dictionary)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.database.close()
        super().tearDownClass()

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


if __name__ == "__main__":
    unittest.main()
