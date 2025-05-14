from enum import Enum
from sqldatabase import (
    SqlDataTypes,
    SqlColumn,
    SqlColumns,
    SqlColumnsWithID,
    SqliteDatabase,
    SqlServerDatabase,
    SqlTable,
    SqlTables,
)


class EPartOfSpeech(Enum):
    NOUN = "noun"
    VERB = "verb"
    ADJECTIVE = "adjective"
    ADVERB = "adverb"
    PRONOUN = "pronoun"
    PREPOSITION = "preposition"
    CONJUNCTION = "conjunction"
    INTERJECTION = "interjection"
    DETERMINER = "determiner"


class ETag(Enum):
    ACTION = "action"
    BUSINESS = "business"
    EVERYDAY = "everyday"
    SCIENCE = "science"
    WEIGHT = "weight"


class WordsTableColumns(SqlColumnsWithID):
    WORD = SqlColumn("word", SqlDataTypes.TEXT, not_null=True, unique=True)
    PRONUNCIATION = SqlColumn("pronunciation", SqlDataTypes.TEXT)


class WordsTable(SqlTable[WordsTableColumns]):
    name = "words"
    columns = WordsTableColumns()


class MeaningsTableColumns(SqlColumnsWithID):
    WORD_ID = SqlColumn(
        "word_id", SqlDataTypes.INTEGER, not_null=True, reference=WordsTable.columns.ID
    )
    DEFINITION = SqlColumn("definition", SqlDataTypes.TEXT, not_null=True)
    PART_OF_SPEECH = SqlColumn(
        "part_of_speech", SqlDataTypes.TEXT, values=EPartOfSpeech
    )


class MeaningsTable(SqlTable[MeaningsTableColumns]):
    name = "meanings"
    columns = MeaningsTableColumns()


class ExamplesTableColumns(SqlColumnsWithID):
    MEANING_ID = SqlColumn(
        "meaning_id",
        SqlDataTypes.INTEGER,
        not_null=True,
        reference=MeaningsTable.columns.ID,
    )
    EXAMPLE = SqlColumn("example", SqlDataTypes.TEXT)


class ExamplesTable(SqlTable[ExamplesTableColumns]):
    name = "examples"
    columns = ExamplesTableColumns()


class TagsTableColumns(SqlColumnsWithID):
    TAG = SqlColumn("tag", SqlDataTypes.TEXT, not_null=True, unique=True, values=ETag)


class TagsTable(SqlTable[TagsTableColumns]):
    name = "tags"
    columns = TagsTableColumns()


class MeaningTagsTableColumns(SqlColumns):
    MEANING_ID = SqlColumn(
        "meaning_id",
        SqlDataTypes.INTEGER,
        not_null=True,
        reference=MeaningsTable.columns.ID,
    )
    TAG_ID = SqlColumn(
        "tag_id", SqlDataTypes.INTEGER, not_null=True, reference=TagsTable.columns.ID
    )


class MeaningTagsTable(SqlTable[MeaningTagsTableColumns]):
    name = "meaning_tags"
    columns = MeaningTagsTableColumns()


class UsersTableColumns(SqlColumnsWithID):
    USERNAME = SqlColumn("username", SqlDataTypes.TEXT, not_null=True, unique=True)
    EMAIL = SqlColumn("email", SqlDataTypes.TEXT, not_null=True, unique=True)


class UsersTable(SqlTable[UsersTableColumns]):
    name = "users"
    columns = UsersTableColumns()


class UserProgressTableColumns(SqlColumns):
    USER_ID = SqlColumn(
        "user_id", SqlDataTypes.INTEGER, not_null=True, reference=UsersTable.columns.ID
    )
    MEANING_ID = SqlColumn(
        "meaning_id",
        SqlDataTypes.INTEGER,
        not_null=True,
        reference=MeaningsTable.columns.ID,
    )
    ATTEMPTS = SqlColumn("attempts", SqlDataTypes.INTEGER, default_value=0)
    CORRECT = SqlColumn("correct", SqlDataTypes.INTEGER, default_value=0)
    LAST_SEEN = SqlColumn("last_seen", SqlDataTypes.DATE)


class UserProgressTable(SqlTable[UserProgressTableColumns]):
    name = "user_progress"
    columns = UserProgressTableColumns()


class DictionaryDatabaseTables(SqlTables):
    WORDS = WordsTable()
    MEANIGS = MeaningsTable()
    EXAMPLES = ExamplesTable()
    TAGS = TagsTable()
    MEANING_TAGS = MeaningTagsTable()
    USERS = UsersTable()
    USER_PROGRESS = UserProgressTable()


class DictionarySqliteDatabase(SqliteDatabase[DictionaryDatabaseTables]):
    tables = DictionaryDatabaseTables()


class DictionarySqlServerDatabase(SqlServerDatabase[DictionaryDatabaseTables]):
    tables = DictionaryDatabaseTables()
