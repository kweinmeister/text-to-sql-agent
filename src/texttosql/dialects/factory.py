from ..config import DB_DIALECT
from .dialect import DatabaseDialect
from .sqlite import SQLiteDialect


def get_dialect() -> DatabaseDialect:
    """
    Returns the configured DatabaseDialect based on the DB_DIALECT env var.
    Defaults to 'sqlite' if not set.
    """
    dialect_name = DB_DIALECT

    if dialect_name == "sqlite":
        return SQLiteDialect()

    elif dialect_name == "postgresql":
        from .postgres import PostgreSQLDialect

        return PostgreSQLDialect()

    raise ValueError(f"Unsupported DB_DIALECT: {dialect_name}")
