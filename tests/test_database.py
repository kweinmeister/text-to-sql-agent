import sqlite3

import pytest

from texttosql.dialects.postgres import PostgreSQLDialect
from texttosql.dialects.sqlite import SQLiteDialect


@pytest.fixture
def temp_sqlite_db(tmp_path) -> str:
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


def test_sqlite_get_ddl(temp_sqlite_db: str):
    """
    Tests that the SQLiteDialect's get_ddl method correctly extracts
    CREATE TABLE statements from a live database.
    """
    dialect = SQLiteDialect()
    ddl = dialect.get_ddl(temp_sqlite_db)

    # Normalize whitespace for consistent comparison
    cleaned_ddl = " ".join(ddl.split())

    assert 'CREATE TABLE "customer"' in cleaned_ddl
    assert '"customer_id" INTEGER NOT NULL' in cleaned_ddl
    assert '"email" TEXT' in cleaned_ddl
    assert 'PRIMARY KEY ("customer_id")' in cleaned_ddl
    assert 'CREATE TABLE "product"' in cleaned_ddl
    assert '"product_id" INTEGER NOT NULL' in cleaned_ddl
    assert 'PRIMARY KEY ("product_id")' in cleaned_ddl


def test_postgres_ddl_generation_logic():
    """
    Tests the logic of PostgreSQL's DDL generation helpers.
    This is an approximation since we don't spin up a live PG database.
    It tests the `_build_ddl_from_info_schema` logic by mocking the cursor.
    """

    # Mock cursor object
    class MockCursor:
        def __init__(self):
            self.queries = []

        def execute(self, query, params=None):
            # Store the query for inspection if needed
            self.queries.append(str(query))

        def fetchall(self):
            # Return data based on which query was last executed
            last_query = self.queries[-1]
            if "information_schema.tables" in last_query:
                return [("customer",)]
            if "information_schema.columns" in last_query:
                return [
                    ("customer_id", "integer", "NO"),
                    ("email", "character varying", "YES"),
                ]
            if "PRIMARY KEY" in last_query:
                return [("customer_id",)]
            if "FOREIGN KEY" in last_query:
                return []
            return []

    dialect = PostgreSQLDialect()
    mock_cursor = MockCursor()

    # We test the internal helper method directly
    ddl = dialect._build_ddl_from_info_schema(mock_cursor)

    # Normalize whitespace
    cleaned_ddl = " ".join(ddl.split())

    assert 'CREATE TABLE "customer"' in cleaned_ddl
    assert '"customer_id" INTEGER NOT NULL' in cleaned_ddl
    assert '"email" TEXT' in cleaned_ddl
    assert 'PRIMARY KEY ("customer_id")' in cleaned_ddl
