import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from sqldatabase import (
    ESqlComparisonOperator,
    ESqlJoinType,
    ESqlOrderByType,
    SqlColumn,
    SqlCondition,
    SqlDatabase,
    SqlJoin,
    SqlRecord,
    SqlSelectStatement,
    SqlTable,
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
    def load_test_dictionary(cls) -> dict:
        dictionary = cls.load_json_data("test_dictionary.json")
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
    def insert_test_dictionary(cls, dictionary: dict) -> None:
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
        for table in self.database.tables:
            for column in table.columns:
                if column.reference is not None:
                    _, table_name, column_name = (
                        column.reference.fully_qualified_name.split(".")
                    )
                    referenced_column = self.database.tables(table_name).columns(
                        column_name
                    )
                    with self.subTest(
                        foreign_key_column=column.fully_qualified_name,
                        referenced_column=referenced_column.fully_qualified_name,
                    ):
                        self.assertIs(column.reference, referenced_column)
                        self.assertIn(column, referenced_column._foreign_keys)

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
        record_count = self.database.tables.WORDS.record_count()
        self.assertEqual(record_count, 3)

    def _print_records(self, records: list[SqlRecord]) -> None:
        for record in records:
            print(record.to_json())

    def _test_records(self, records: list[SqlRecord]) -> None:
        expected_records = [
            SqlRecord.from_json(data, self.database)
            for data in self.test_data[self.test_name]
        ]
        for record, expected_record in zip(records, expected_records):
            self.assertEqual(record, expected_record)

    def _test_select_words_table_records(self) -> None:
        records = self.database.tables.WORDS.select_records()
        self._test_records(records)

    def _test_select_word_meanings(self) -> None:
        word = "book"
        words_table = self.database.tables.WORDS
        meanings_table = self.database.tables.MEANIGS
        records = meanings_table.select_records(
            where_condition=SqlCondition(
                meanings_table.columns.WORD_ID,
                ESqlComparisonOperator.IS_EQUAL,
                SqlSelectStatement(
                    self.database.dialect,
                    words_table,
                    words_table.columns.ID,
                    where_condition=words_table.columns.WORD.filters.IS_EQUAL(word),
                ),
            )
        )
        self._test_records(records)

    def _test_select_word_definitions(self) -> None:
        word = "run"
        words_table = self.database.tables.WORDS
        meanings_table = self.database.tables.MEANIGS
        records = words_table.select_records(
            words_table.columns.WORD,
            meanings_table.columns.PART_OF_SPEECH,
            meanings_table.columns.DEFINITION,
            joins=[
                words_table.join(meanings_table),
            ],
            where_condition=words_table.columns.WORD.filters.IS_EQUAL(word),
        )
        self._test_records(records)

    def _test_select_word_examples(self) -> None:
        word = "light"
        words_table = self.database.tables.WORDS
        meanings_table = self.database.tables.MEANIGS
        examples_table = self.database.tables.EXAMPLES
        records = words_table.select_records(
            words_table.columns.WORD,
            meanings_table.columns.PART_OF_SPEECH,
            examples_table.columns.EXAMPLE,
            joins=[
                words_table.join(meanings_table),
                meanings_table.join(examples_table),
            ],
            where_condition=words_table.columns.WORD.filters.IS_EQUAL(word),
        )
        self._test_records(records)

    def _test_select_words_tags(self) -> None:
        words_table = self.database.tables.WORDS
        meanings_table = self.database.tables.MEANIGS
        tags_table = self.database.tables.TAGS
        meaning_tags_table = self.database.tables.MEANING_TAGS
        records = words_table.select_records(
            words_table.columns.WORD,
            tags_table.columns.TAG,
            joins=[
                words_table.join(meanings_table),
                meanings_table.join(meaning_tags_table),
                meaning_tags_table.join(tags_table),
            ],
        )
        word_tags: dict[str, list[str]] = {}
        for record in records:
            word = record[words_table.columns.WORD]
            tag = record[tags_table.columns.TAG]
            tags = word_tags.get(word, [])
            tags.append(tag.value)
            word_tags[word] = tags
        expected_word_tags = self.test_data[self.test_name]
        self.assertEqual(word_tags, expected_word_tags)

    def _test_select_meanings_never_seen_by_user(self) -> None:
        user_id = 1
        words_table = self.database.tables.WORDS
        meanings_table = self.database.tables.MEANIGS
        user_progress_table = self.database.tables.USER_PROGRESS
        records = meanings_table.select_records(
            words_table.columns.WORD,
            meanings_table.columns.PART_OF_SPEECH,
            meanings_table.columns.DEFINITION,
            joins=[
                meanings_table.join(words_table),
            ],
            where_condition=SqlCondition(
                meanings_table.columns.ID,
                ESqlComparisonOperator.IS_NOT_IN,
                SqlSelectStatement(
                    self.database.dialect,
                    user_progress_table,
                    user_progress_table.columns.MEANING_ID,
                    where_condition=user_progress_table.columns.USER_ID.filters.IS_EQUAL(
                        user_id
                    ),
                ),
            ),
        )
        self._test_records(records)

    def _test_select_users_ordered_by_correct_answers(self) -> None:
        user_table = self.database.tables.USERS
        user_progress_table = self.database.tables.USER_PROGRESS
        sum_correct_function = self.database.functions.SUM(
            user_progress_table.columns.CORRECT
        )
        records = user_table.select_records(
            user_table.columns.USERNAME,
            sum_correct_function,
            joins=[
                user_table.join(user_progress_table),
            ],
            group_by_columns=[user_table.columns.ID],
            order_by_items=[sum_correct_function, ESqlOrderByType.DESCENDING],
        )
        self._test_records(records)

    def _test_select_word_meanings_count(self) -> None:
        words_table = self.database.tables.WORDS
        meanings_table = self.database.tables.MEANIGS
        count_meanings_function = self.database.functions.COUNT(
            meanings_table.columns.ID
        )
        records = words_table.select_records(
            words_table.columns.WORD,
            count_meanings_function,
            joins=[
                words_table.join(meanings_table, ESqlJoinType.LEFT),
            ],
            group_by_columns=[words_table.columns.ID],
        )
        self._test_records(records)

    def _test_select_user_answers_accuracy(self) -> None:
        user_table = self.database.tables.USERS
        user_progress_table = self.database.tables.USER_PROGRESS
        sum_attempts_function = self.database.functions.SUM(
            user_progress_table.columns.ATTEMPTS
        )
        sum_correct_function = self.database.functions.SUM(
            user_progress_table.columns.CORRECT
        )
        records = user_progress_table.select_records(
            user_table.columns.USERNAME,
            sum_attempts_function,
            sum_correct_function,
            joins=[
                user_progress_table.join(user_table),
            ],
            group_by_columns=[user_table.columns.ID],
        )
        users_accuracy = {
            record[user_table.columns.USERNAME]: round(
                (record[sum_correct_function] / record[sum_attempts_function]) * 100, 2
            )
            for record in records
        }
        expected_users_accuracy = self.test_data[self.test_name]
        self.assertEqual(users_accuracy, expected_users_accuracy)

    def _test_insert_word_entry(self) -> None:
        word_entry = {
            "word": "jump",
            "pronunciation": "dʒʌmp",
            "meanings": [
                {
                    "part_of_speech": EPartOfSpeech.VERB,
                    "definition": "push oneself off a surface into the air",
                    "examples": [
                        "He jumped over the fence easily.",
                        "The cat jumped onto the windowsill.",
                        "She jumped when she heard the loud noise.",
                    ],
                }
            ],
        }
        words_table = self.database.tables.WORDS
        meanings_table = self.database.tables.MEANIGS
        examples_table = self.database.tables.EXAMPLES
        words_ids = words_table.insert_records(
            SqlRecord(
                {
                    words_table.columns.WORD: word_entry["word"],
                    words_table.columns.PRONUNCIATION: word_entry["pronunciation"],
                }
            )
        )
        assert (
            words_ids is not None and len(words_ids) == 1
        ), "No inserted word record id returned."
        word_id = words_ids[0]
        self.assertEqual(word_id, 4)
        meaning_recrods = []
        for meaning_entry in word_entry["meanings"]:
            meaning_recrods.append(
                SqlRecord(
                    {
                        meanings_table.columns.WORD_ID: word_id,
                        meanings_table.columns.PART_OF_SPEECH: meaning_entry["part_of_speech"],  # type: ignore
                        meanings_table.columns.DEFINITION: meaning_entry["definition"],  # type: ignore
                    }
                )
            )
        meaning_ids = meanings_table.insert_records(meaning_recrods)
        assert (
            meaning_ids is not None and len(meaning_ids) > 0
        ), "No inserted meaning records ids returned."
        self.assertEqual(meaning_ids, [7])
        example_records = []
        for meaning_id, meaning_entry in zip(meaning_ids, word_entry["meanings"]):
            for example in meaning_entry["examples"]:  # type: ignore
                example_records.append(
                    SqlRecord(
                        {
                            examples_table.columns.MEANING_ID: meaning_id,
                            examples_table.columns.EXAMPLE: example,
                        }
                    )
                )
        example_ids = examples_table.insert_records(example_records)
        self.assertEqual(example_ids, [7, 8, 9])
        examples_record_count = examples_table.record_count()
        self.assertEqual(examples_record_count, 9)

    def _test_update_correct_answers_count(self) -> None:
        user_id = 1
        meaning_id = 1
        user_progress_table = self.database.tables.USER_PROGRESS
        where_condition = user_progress_table.columns.USER_ID.filters.IS_EQUAL(
            user_id
        ) & user_progress_table.columns.MEANING_ID.filters.IS_EQUAL(meaning_id)
        correct_answers_count = user_progress_table.select_records(
            user_progress_table.columns.CORRECT, where_condition=where_condition
        )[0][user_progress_table.columns.CORRECT]
        new_correct_answers_count = correct_answers_count + 1
        ids = user_progress_table.update_records(
            SqlRecord(
                {
                    user_progress_table.columns.CORRECT: new_correct_answers_count,
                }
            ),
            where_condition=where_condition,
        )
        self.assertIsNone(ids)
        correct_answers_count = user_progress_table.select_records(
            user_progress_table.columns.CORRECT, where_condition=where_condition
        )[0][user_progress_table.columns.CORRECT]
        self.assertEqual(correct_answers_count, new_correct_answers_count)

    def _test_delete_user_and_user_progress(self) -> None:
        user_id = 2
        users_table = self.database.tables.USERS
        user_progress_table = self.database.tables.USER_PROGRESS
        user_ids = users_table.delete_records(
            users_table.columns.ID.filters.IS_EQUAL(user_id)
        )
        self.assertEqual(user_ids, [user_id])
        users_count = users_table.record_count()
        self.assertEqual(users_count, 1)
        user_progress_ids = user_progress_table.delete_records(
            user_progress_table.columns.USER_ID.filters.IS_EQUAL(user_id)
        )
        self.assertIsNone(user_progress_ids)
        user_progresses_cout = user_progress_table.record_count()
        self.assertEqual(user_progresses_cout, 3)
