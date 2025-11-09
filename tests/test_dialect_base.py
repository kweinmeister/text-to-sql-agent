from typing import Any
from unittest.mock import MagicMock, patch

from pytest_mock import MockerFixture
from sqlglot import exp

from texttosql.dialects.dialect import DatabaseDialect


class ConcreteDatabaseDialect(DatabaseDialect):  # type: ignore
    """A concrete implementation for testing the base class."""

    @property
    def name(self) -> str:
        return "test"

    def get_connection(self, db_uri: str) -> Any:
        pass

    def get_sqlglot_dialect(self) -> str:
        return "test"

    def _get_ddl_from_db(self, db_uri: str) -> str:
        return "CREATE TABLE test (id INTEGER);"

    def map_type_to_ddl(self, sql_type: str) -> str:
        return "TEST"


def test_base_class_caching(mocker: MockerFixture) -> None:
    """Test that the base class caching works correctly."""
    # Mock the internal method to track calls
    mock_get_ddl = mocker.patch.object(
        ConcreteDatabaseDialect,
        "_get_ddl_from_db",
        return_value="CREATE TABLE test (id INTEGER);",
    )
    dialect = ConcreteDatabaseDialect()

    # First call should hit the internal method
    ddl1 = dialect.get_ddl("test_uri")
    mock_get_ddl.assert_called_once_with("test_uri")

    # Second call should use cache
    ddl2 = dialect.get_ddl("test_uri")
    mock_get_ddl.assert_called_once()  # Still only called once

    assert ddl1 == ddl2


def test_base_class_sqlglot_schema_caching(mocker: MockerFixture) -> None:
    """Test that the SQLGlot schema caching works correctly."""
    # Mock the internal methods to track calls
    mocker.patch.object(
        ConcreteDatabaseDialect,
        "_get_ddl_from_db",
        return_value="CREATE TABLE test (id INTEGER);",
    )
    mock_parse = mocker.patch.object(
        ConcreteDatabaseDialect,
        "_parse_ddl_to_sqlglot_schema",
        return_value={"test": {"id": "INTEGER"}},
    )
    dialect = ConcreteDatabaseDialect()

    # First call should parse
    schema1 = dialect.get_sqlglot_schema("test_uri")
    mock_parse.assert_called_once()

    # Second call should use cache
    schema2 = dialect.get_sqlglot_schema("test_uri")
    mock_parse.assert_called_once()  # Still only called once

    assert schema1 == schema2


def test_parse_ddl_to_sqlglot_schema_with_empty_ddl() -> None:
    """Test parsing with empty DDL."""
    dialect = ConcreteDatabaseDialect()
    schema = dialect._parse_ddl_to_sqlglot_schema("")
    assert schema == {}


def test_parse_ddl_to_sqlolot_schema_with_malformed_statement() -> None:
    """Test parsing with a malformed statement that causes parse to return None."""
    dialect = ConcreteDatabaseDialect()

    # Create a DDL string that will cause parse to return None or empty list
    malformed_ddl = "INVALID SQL STATEMENT;"

    with patch("texttosql.dialects.dialect.parse", return_value=None):
        schema = dialect._parse_ddl_to_sqlglot_schema(malformed_ddl)
        assert schema == {}


def test_parse_ddl_to_sqlglot_schema_with_create_without_schema() -> None:
    """Test parsing when create expression doesn't have a schema."""
    dialect = ConcreteDatabaseDialect()

    # Mock parse to return a Create expression without a schema
    mock_create = MagicMock(spec=exp.Create)
    mock_create.kind = "TABLE"
    mock_table = MagicMock()
    mock_table.name = "test"
    mock_create.this = mock_table

    with patch("texttosql.dialects.dialect.parse", return_value=[mock_create]):
        schema = dialect._parse_ddl_to_sqlglot_schema("CREATE TABLE test (id INTEGER);")
        # Should handle the case where there's no schema - table should not be added
        assert "test" not in schema


def test_parse_ddl_to_sqlglot_schema_with_column_without_kind() -> None:
    """Test parsing when a column definition doesn't have a type."""
    dialect = ConcreteDatabaseDialect()

    # Create a proper mock structure that matches the actual code logic
    mock_create = MagicMock(spec=exp.Create)
    mock_create.kind = "TABLE"

    # Create a mock table object with a name attribute
    mock_table = MagicMock()
    mock_table.name = "test"

    # Create a mock schema with the table as its 'this' attribute
    mock_schema = MagicMock(spec=exp.Schema)
    mock_schema.this = mock_table
    mock_create.this = mock_schema

    # Create column definitions
    mock_col_def1 = MagicMock(spec=exp.ColumnDef)
    mock_col_def1.this.name = "id"
    mock_col_def1.kind = MagicMock()
    mock_col_def1.kind.sql.return_value = "INTEGER"

    mock_col_def2 = MagicMock(spec=exp.ColumnDef)
    mock_col_def2.this.name = "name"
    mock_col_def2.kind = None  # No type

    mock_schema.expressions = [mock_col_def1, mock_col_def2]

    with patch("texttosql.dialects.dialect.parse", return_value=[mock_create]):
        schema = dialect._parse_ddl_to_sqlglot_schema(
            "CREATE TABLE test (id INTEGER, name);"
        )

        # Should handle the case where a column has no type
        assert "test" in schema
        assert schema["test"]["name"] == "UNKNOWN"


def test_parse_ddl_to_sqlglot_schema_with_exception() -> None:
    """Test that exceptions in parsing are handled gracefully."""
    dialect = ConcreteDatabaseDialect()

    ddl = "CREATE TABLE test (id INTEGER);"

    with patch(
        "texttosql.dialects.dialect.parse", side_effect=Exception("Parse error")
    ):
        schema = dialect._parse_ddl_to_sqlglot_schema(ddl)
        # Should return empty schema when parsing fails
        assert schema == {}


def test_parse_ddl_to_sqlglot_schema_with_quoted_table_name() -> None:
    """Test parsing with quoted table names."""
    dialect = ConcreteDatabaseDialect()

    ddl = 'CREATE TABLE "test" (id INTEGER);'

    # The normal parsing should work with quoted names
    # We're just testing that it doesn't crash
    dialect._parse_ddl_to_sqlglot_schema(ddl)
    # This test is mainly to ensure no exceptions are raised
