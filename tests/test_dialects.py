import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from texttosql.dialects.factory import get_dialect
from texttosql.dialects.postgres import PostgreSQLDialect
from texttosql.dialects.sqlite import SQLiteDialect


@pytest.fixture
def temp_sqlite_db(tmp_path: Path) -> str:
    """Create a temporary SQLite database for testing."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # A simple schema to test against
    cursor.execute("""
    CREATE TABLE customer (
        customer_id INTEGER NOT NULL PRIMARY KEY,
        email TEXT
    );
    """)
    cursor.execute("""
    CREATE TABLE product (
        product_id INTEGER NOT NULL,
        name TEXT,
        PRIMARY KEY (product_id)
    );
    """)
    conn.commit()
    conn.close()
    return str(db_path)


@patch("texttosql.dialects.factory.DB_DIALECT", "sqlite")
def test_get_dialect_returns_sqlite() -> None:
    """Test that get_dialect returns SQLiteDialect for 'sqlite'."""
    dialect = get_dialect()
    assert isinstance(dialect, SQLiteDialect)


@patch("texttosql.dialects.factory.DB_DIALECT", "postgresql")
def test_get_dialect_returns_postgres() -> None:
    """Test that get_dialect returns PostgreSQLDialect for 'postgresql'."""
    dialect = get_dialect()
    assert isinstance(dialect, PostgreSQLDialect)


@patch("texttosql.dialects.factory.DB_DIALECT", "unsupported_db")
def test_get_dialect_raises_error_for_unsupported_dialect() -> None:
    """Test that get_dialect raises a ValueError for an unsupported dialect."""
    with pytest.raises(ValueError, match="Unsupported DB_DIALECT: unsupported_db"):
        get_dialect()


def test_ddl_and_schema_caching(temp_sqlite_db: str) -> None:
    """Verify that database-hitting and parsing methods are only called once."""
    dialect = SQLiteDialect()

    # Spy on the internal, uncached methods
    dialect._get_ddl_from_db = MagicMock(wraps=dialect._get_ddl_from_db)
    dialect._parse_ddl_to_sqlglot_schema = MagicMock(
        wraps=dialect._parse_ddl_to_sqlglot_schema
    )

    # --- First call to get_ddl ---
    # This should trigger both _get_ddl_from_db and _parse_ddl_to_sqlglot_schema
    ddl1 = dialect.get_ddl(temp_sqlite_db)
    dialect._get_ddl_from_db.assert_called_once_with(temp_sqlite_db)
    dialect._parse_ddl_to_sqlglot_schema.assert_called_once()

    # --- Second call to get_ddl ---
    # Should be fully cached, no new calls
    ddl2 = dialect.get_ddl(temp_sqlite_db)
    dialect._get_ddl_from_db.assert_called_once()
    dialect._parse_ddl_to_sqlglot_schema.assert_called_once()
    assert ddl1 == ddl2

    # --- First call to get_sqlglot_schema ---
    # Should also be fully cached from the first get_ddl call
    schema1 = dialect.get_sqlglot_schema(temp_sqlite_db)
    dialect._get_ddl_from_db.assert_called_once()
    dialect._parse_ddl_to_sqlglot_schema.assert_called_once()

    # --- Second call to get_sqlglot_schema ---
    # Still fully cached
    schema2 = dialect.get_sqlglot_schema(temp_sqlite_db)
    dialect._get_ddl_from_db.assert_called_once()
    dialect._parse_ddl_to_sqlglot_schema.assert_called_once()

    assert schema1 is not None
    assert schema1 == schema2


def test_parse_ddl_to_sqlglot_schema_resilience() -> None:
    """Test that the DDL parser can handle a bad statement among good ones."""
    dialect = SQLiteDialect()
    malformed_ddl = """
    CREATE TABLE "good_table" (
        "id" INTEGER,
        "name" TEXT
    );

    CREATE GARBAGE SYNTAX; -- This is an invalid statement

    CREATE TABLE "another_good_table" (
        "value" REAL
    );
    """

    schema = dialect._parse_ddl_to_sqlglot_schema(malformed_ddl)

    assert "good_table" in schema
    assert "another_good_table" in schema
    assert schema["good_table"]["id"] == "INTEGER"
    assert len(schema) == 2  # The bad statement should be skipped


def test_sqlite_type_mapping() -> None:
    """Test SQLite's generic type mapping logic."""
    dialect = SQLiteDialect()
    assert dialect.map_type_to_ddl("text") == "TEXT"
    assert dialect.map_type_to_ddl("integer") == "INTEGER"
    assert dialect.map_type_to_ddl("number") == "REAL"
    assert dialect.map_type_to_ddl("boolean") == "BOOLEAN"
    assert dialect.map_type_to_ddl("unknown_type") == "TEXT"  # Default case


def test_postgres_type_to_generic() -> None:
    """Test PostgreSQL's specific type to generic type mapping logic."""
    dialect = PostgreSQLDialect()
    # Test integer types
    assert dialect._postgres_type_to_generic("int") == "INTEGER"
    assert dialect._postgres_type_to_generic("integer") == "INTEGER"
    assert dialect._postgres_type_to_generic("bigint") == "INTEGER"

    # Test text types
    assert dialect._postgres_type_to_generic("char") == "TEXT"
    assert dialect._postgres_type_to_generic("character") == "TEXT"
    assert dialect._postgres_type_to_generic("varchar") == "TEXT"
    assert dialect._postgres_type_to_generic("text") == "TEXT"

    # Test numeric types
    assert dialect._postgres_type_to_generic("numeric") == "NUMBER"
    assert dialect._postgres_type_to_generic("decimal") == "NUMBER"
    assert dialect._postgres_type_to_generic("real") == "NUMBER"
    assert dialect._postgres_type_to_generic("float") == "NUMBER"
    assert dialect._postgres_type_to_generic("double precision") == "NUMBER"

    # Test timestamp types
    assert dialect._postgres_type_to_generic("timestamp") == "TIMESTAMP"
    assert (
        dialect._postgres_type_to_generic("timestamp without time zone") == "TIMESTAMP"
    )

    # Test date types
    assert dialect._postgres_type_to_generic("date") == "DATE"

    # Test boolean types
    assert dialect._postgres_type_to_generic("bool") == "BOOLEAN"
    assert dialect._postgres_type_to_generic("boolean") == "BOOLEAN"

    # Test default case
    assert dialect._postgres_type_to_generic("unknown_type") == "TEXT"


def test_postgres_quote_identifier() -> None:
    """Test PostgreSQL's identifier quoting."""
    dialect = PostgreSQLDialect()
    assert dialect.quote_identifier("table_name") == '"table_name"'
    assert dialect.quote_identifier("column_name") == '"column_name"'


def test_sqlite_quote_identifier() -> None:
    """Test SQLite's identifier quoting."""
    dialect = SQLiteDialect()
    assert dialect.quote_identifier("table_name") == '"table_name"'
    assert dialect.quote_identifier("column_name") == '"column_name"'


def test_postgres_type_mapping() -> None:
    """Test PostgreSQL's generic type mapping logic."""
    dialect = PostgreSQLDialect()
    assert dialect.map_type_to_ddl("text") == "TEXT"
    assert dialect.map_type_to_ddl("number") == "NUMERIC"
    assert dialect.map_type_to_ddl("timestamp") == "TIMESTAMP"
    assert dialect.map_type_to_ddl("date") == "DATE"
    assert dialect.map_type_to_ddl("unknown_type") == "TEXT"  # Default case
