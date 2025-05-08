from sqldatabase import (
    SqlDataTypes,
    SqlColumn,
    SqlColumnsWithID,
    SqliteDatabase,
    SqlServerDatabase,
    SqlTable,
    SqlTables,
)


class UsersTableColumns(SqlColumnsWithID):
    TIMESTAMP = SqlColumn("timestamp", SqlDataTypes.FLOAT, default=0.1)
    USERNAME = SqlColumn("username", SqlDataTypes.TEXT, not_null=True, unique=True)
    EMAIL = SqlColumn("email", SqlDataTypes.TEXT, not_null=True, unique=True)
    PHOTO = SqlColumn("photo", SqlDataTypes.BLOB)
    ADMIN = SqlColumn("admin", SqlDataTypes.BOOLEAN, not_null=True)
    BIRTH_DATE = SqlColumn("birth_date", SqlDataTypes.DATE)
    LAST_LOGIN = SqlColumn("last_login", SqlDataTypes.DATETIME)
    AUTOMATIC_LOGOUT = SqlColumn("automatic_logout", SqlDataTypes.TIME)


class UsersTable(SqlTable[UsersTableColumns]):
    name = "users"
    columns = UsersTableColumns()


class UsersDatabaseTables(SqlTables):
    USERS = UsersTable()


class UsersSqliteDatabase(SqliteDatabase[UsersDatabaseTables]):
    tables = UsersDatabaseTables()


class UsersSqlServerDatabase(SqlServerDatabase[UsersDatabaseTables]):
    tables = UsersDatabaseTables()
