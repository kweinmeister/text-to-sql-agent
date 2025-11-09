from unittest.mock import MagicMock

import pytest

from texttosql.dialects.sqlite import SQLiteDialect
from texttosql.engine import SQLExecutor, SQLValidator


@pytest.fixture
def mock_dialect():
    """A mock dialect for testing."""
    dialect = SQLiteDialect()
    # Mock the sqlglot dialect name to avoid dependency on the concrete class
    dialect.get_sqlglot_dialect = MagicMock(return_value="sqlite")
    return dialect


@pytest.fixture
def sample_schema():
    """A sample sqlglot schema for the validator."""
    return {"customer": {"customer_id": "INTEGER", "first_name": "TEXT"}}


def test_validator_success(mock_dialect, sample_schema):
    """Test SQLValidator with a valid query."""
    validator = SQLValidator()
    sql = "SELECT first_name FROM customer WHERE customer_id = 1;"
    result = validator.validate(sql, mock_dialect, sample_schema)
    assert result["status"] == "success"


def test_validator_syntax_error(mock_dialect, sample_schema):
    """Test SQLValidator with a syntax error."""
    validator = SQLValidator()
    sql = "SELEC first_name FROM customer;"
    result = validator.validate(sql, mock_dialect, sample_schema)
    assert result["status"] == "error"
    # Check that the error message contains information about the parsing error
    assert (
        "Invalid expression" in result["errors"][0]
        or "Unexpected token" in result["errors"][0]
    )


def test_validator_semantic_error_unknown_column(mock_dialect, sample_schema):
    """Test SQLValidator with a non-existent column."""
    validator = SQLValidator()
    sql = "SELECT last_name FROM customer;"  # last_name is not in the schema
    result = validator.validate(sql, mock_dialect, sample_schema)
    assert result["status"] == "error"
    # Check that the error message contains information about the unknown column
    assert "could not be resolved" in result["errors"][0]


def test_executor_success():
    """Test SQLExecutor on a successful query execution."""
    mock_dialect = MagicMock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_dialect.get_connection.return_value = mock_conn
    mock_conn.__enter__.return_value.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [(1, "John")]

    executor = SQLExecutor("dummy_uri", mock_dialect)
    result, error = executor.execute("SELECT * FROM users;")

    assert result == [(1, "John")]
    assert error is None
    mock_dialect.get_connection.assert_called_once_with("dummy_uri")
    mock_cursor.execute.assert_called_once_with("SELECT * FROM users;")
    mock_conn.__enter__.assert_called_once()
    mock_conn.__exit__.assert_called_once()


def test_executor_failure():
    """Test SQLExecutor when the database raises an error."""
    mock_dialect = MagicMock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_dialect.get_connection.return_value = mock_conn
    mock_conn.__enter__.return_value.cursor.return_value = mock_cursor
    mock_cursor.execute.side_effect = Exception("Table not found")

    executor = SQLExecutor("dummy_uri", mock_dialect)
    result, error = executor.execute("SELECT * FROM non_existent_table;")

    assert result is None
    assert error == "Table not found"


def test_executor_with_context_manager():
    """Test that SQLExecutor safely uses context manager."""
    mock_dialect = MagicMock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_dialect.get_connection.return_value = mock_conn
    mock_conn.__enter__.return_value = mock_conn  # Simulate context manager
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [(1, "John")]

    executor = SQLExecutor("dummy_uri", mock_dialect)
    result, error = executor.execute("SELECT * FROM users;")

    assert result == [(1, "John")]
    assert error is None
    mock_conn.__enter__.assert_called_once()
    mock_conn.__exit__.assert_called_once()  # Ensures cleanup
