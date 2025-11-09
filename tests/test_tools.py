from collections.abc import Sequence
from typing import Any, cast
from unittest.mock import Mock, patch

from texttosql.dialects.sqlite import SQLiteDialect
from texttosql.tools import (
    load_schema_into_state,
    run_sql_execution,
    run_sql_validation,
)


def test_load_schema_into_state_success() -> None:
    """Test that load_schema_into_state correctly populates the state."""
    mock_dialect = Mock(spec=SQLiteDialect)
    mock_dialect.get_ddl.return_value = "CREATE TABLE test;"
    mock_dialect.get_sqlglot_schema.return_value = {"test": {"id": "int"}}
    state: dict[str, Any] = {}

    load_schema_into_state(state, mock_dialect)

    assert state["schema_ddl"] == "CREATE TABLE test;"
    assert state["sqlglot_schema"] == {"test": {"id": "int"}}
    mock_dialect.get_ddl.assert_called_once()
    mock_dialect.get_sqlglot_schema.assert_called_once()


def test_load_schema_into_state_failure() -> None:
    """Test load_schema_into_state handles exceptions."""
    mock_dialect = Mock(spec=SQLiteDialect)
    mock_dialect.get_ddl.side_effect = Exception("DB connection failed")
    state: dict[str, Any] = {}

    load_schema_into_state(state, mock_dialect)

    assert (
        "Error loading schema: Error extracting schema: DB connection failed"
        in state["schema_ddl"]
    )
    assert "DB connection failed" in state["sqlglot_schema"]["error"]


@patch("texttosql.engine.SQLValidator")
def test_run_sql_validation_success(MockSQLValidator: Mock) -> None:
    """Test run_sql_validation tool on success."""
    mock_dialect = Mock(spec=SQLiteDialect)
    mock_validator_instance = MockSQLValidator.return_value
    mock_validator_instance.validate.return_value = {"status": "success"}

    state = {"sql_query": "SELECT 1;", "sqlglot_schema": {"some": "schema"}}

    result = run_sql_validation(state, mock_dialect)

    assert result["status"] == "success"
    assert cast(dict, state["validation_result"])["status"] == "success"
    mock_validator_instance.validate.assert_called_once()


def test_run_sql_validation_no_query() -> None:
    """Test run_sql_validation when no query is in the state."""
    mock_dialect = Mock(spec=SQLiteDialect)
    state = {"sqlglot_schema": {"some": "schema"}}
    result = run_sql_validation(state, mock_dialect)
    assert result["status"] == "error"
    assert "No SQL query found" in cast(Sequence[str], result["errors"])[0]


def test_load_schema_into_state_no_db_uri() -> None:
    """Test load_schema_into_state when DB_URI is not set."""
    mock_dialect = Mock(spec=SQLiteDialect)
    with patch("texttosql.tools.DB_URI", None):
        state: dict[str, Any] = {}
        load_schema_into_state(state, mock_dialect)

        assert (
            "Error loading schema: Error: DB_URI environment variable not set."
            in state["schema_ddl"]
        )
        assert (
            "Error: DB_URI environment variable not set."
            in state["sqlglot_schema"]["error"]
        )


@patch("texttosql.engine.SQLExecutor")
def test_run_sql_execution_success(MockSQLExecutor: Mock) -> None:
    """Test run_sql_execution tool on success."""
    mock_dialect = Mock(spec=SQLiteDialect)
    mock_executor_instance = MockSQLExecutor.return_value
    mock_executor_instance.execute.return_value = ([(1, "John")], None)

    with patch("texttosql.tools.DB_URI", "dummy_uri"):
        state = {"sql_query": "SELECT * FROM users;"}

        result = run_sql_execution(state, mock_dialect)

        assert result["status"] == "success"
        assert result["result"] == [(1, "John")]
        assert cast(dict[str, Any], state["execution_result"])["status"] == "success"
        assert cast(dict[str, Any], state["execution_result"])["result"] == [
            (1, "John")
        ]
        mock_executor_instance.execute.assert_called_once_with("SELECT * FROM users;")


@patch("texttosql.engine.SQLExecutor")
def test_run_sql_execution_failure(MockSQLExecutor: Mock) -> None:
    """Test run_sql_execution tool on failure."""
    mock_dialect = Mock(spec=SQLiteDialect)
    mock_executor_instance = MockSQLExecutor.return_value
    mock_executor_instance.execute.return_value = (None, "Table not found")

    with patch("texttosql.tools.DB_URI", "dummy_uri"):
        state = {"sql_query": "SELECT * FROM non_existent_table;"}

        result = run_sql_execution(state, mock_dialect)

        assert result["status"] == "error"
        assert result["error_message"] == "Table not found"
        assert cast(dict[str, Any], state["execution_result"])["status"] == "error"
        assert (
            cast(dict[str, Any], state["execution_result"])["error_message"]
            == "Table not found"
        )


def test_run_sql_execution_no_query() -> None:
    """Test run_sql_execution when no query is in the state."""
    mock_dialect = Mock(spec=SQLiteDialect)
    state: dict[str, Any] = {}
    result = run_sql_execution(state, mock_dialect)
    assert result["status"] == "error"
    assert "No SQL query found" in result["error_message"]
    assert cast(dict[str, Any], state["execution_result"])["status"] == "error"
    assert (
        "No SQL query found"
        in cast(dict[str, Any], state["execution_result"])["error_message"]
    )


@patch("texttosql.tools.DB_URI", None)
def test_run_sql_execution_no_db_uri() -> None:
    """Test run_sql_execution when DB_URI is not set."""
    mock_dialect = Mock(spec=SQLiteDialect)
    state = {"sql_query": "SELECT * FROM users;"}
    result = run_sql_execution(state, mock_dialect)
    assert result["status"] == "error"
    assert result["error_message"] == "DB_URI not set."
    # When DB_URI is not set, the function returns early and doesn't set execution_result in state
    assert "execution_result" not in state


def test_load_schema_into_state_sqlglot_schema_error() -> None:
    """Test load_schema_into_state when sqlglot schema loading fails."""
    mock_dialect = Mock(spec=SQLiteDialect)
    mock_dialect.get_ddl.return_value = "CREATE TABLE test;"
    mock_dialect.get_sqlglot_schema.side_effect = Exception("Schema parsing failed")
    state: dict[str, Any] = {}

    load_schema_into_state(state, mock_dialect)

    assert (
        "Error loading schema: Error extracting schema: Schema parsing failed"
        in state["schema_ddl"]
    )
    assert "Schema parsing failed" in state["sqlglot_schema"]["error"]
