import logging
from abc import ABC, abstractmethod
from typing import Any

from sqlglot import exp, parse


class DatabaseDialect(ABC):
    """Abstract base class defining database-specific behaviors."""

    def __init__(self) -> None:
        self._schema_cache: dict[str, dict[str, Any]] = {}

    # --- Public API (with caching) ---

    def get_ddl(self, db_uri: str) -> str:
        """
        Extracts the database schema as a DDL string, using a cache.
        """
        if db_uri not in self._schema_cache or "ddl" not in self._schema_cache[db_uri]:
            self._ensure_schema_cached(db_uri)
        return self._schema_cache[db_uri]["ddl"]

    def get_sqlglot_schema(self, db_uri: str) -> dict[str, dict[str, str]]:
        """
        Builds a SQLGlot MappingSchema by parsing the database's DDL, using a cache.
        """
        if (
            db_uri not in self._schema_cache
            or "sqlglot_schema" not in self._schema_cache[db_uri]
        ):
            self._ensure_schema_cached(db_uri)
        return self._schema_cache[db_uri]["sqlglot_schema"]

    def _ensure_schema_cached(self, db_uri: str) -> None:
        """
        Ensures both DDL and SQLGlot schema are cached for a given db_uri.
        This is the single point of entry for populating the cache.
        """
        if (
            db_uri in self._schema_cache
            and "ddl" in self._schema_cache[db_uri]
            and "sqlglot_schema" in self._schema_cache[db_uri]
        ):
            return  # Already fully cached

        ddl_string = self._get_ddl_from_db(db_uri)
        sqlglot_schema = self._parse_ddl_to_sqlglot_schema(ddl_string)

        self._schema_cache[db_uri] = {
            "ddl": ddl_string,
            "sqlglot_schema": sqlglot_schema,
        }

    # --- Abstract methods for subclasses to implement ---

    @property
    @abstractmethod
    def name(self) -> str:
        """Return database name (e.g., 'sqlite')."""
        pass

    @abstractmethod
    def get_connection(self, db_uri: str) -> Any:
        """Create a database connection."""
        pass

    @abstractmethod
    def get_sqlglot_dialect(self) -> str:
        """Return the dialect name compatible with SQLGlot."""
        pass

    @abstractmethod
    def _get_ddl_from_db(self, db_uri: str) -> str:
        """
        The actual implementation for extracting the DDL from the database.
        This is called by the public get_ddl method.
        """
        pass

    @abstractmethod
    def map_type_to_ddl(self, sql_type: str) -> str:
        """Map a generic type to a database-specific DDL type."""
        pass

    # --- Private/Protected Helper Methods ---

    def _parse_ddl_to_sqlglot_schema(
        self, ddl_string: str
    ) -> dict[str, dict[str, str]]:
        """
        Parses a DDL string into the SQLGlot schema dictionary format.
        It splits the DDL into individual CREATE TABLE statements and parses them one by one
        to be more resilient to errors in a single statement.
        """
        sqlglot_schema: dict[str, dict[str, str]] = {}
        # Split statements by the semicolon that typically ends a CREATE TABLE block.
        # This is more robust than splitting by "\n\n" for some DDL formats.
        statements = [s.strip() for s in ddl_string.split(";") if s.strip()]

        for statement in statements:
            try:
                # The parse function can handle a list of expressions, but we parse one at a time
                # to isolate errors.
                create_expr_list = parse(statement, read=self.get_sqlglot_dialect())
                if not create_expr_list:
                    continue

                create_expr = create_expr_list[0]

                if isinstance(create_expr, exp.Create) and create_expr.kind == "TABLE":
                    # For quoted table names, we need to get the name from the nested Table object
                    table_name = (
                        create_expr.this.this.name
                        if hasattr(create_expr.this, "this")
                        and hasattr(create_expr.this.this, "name")
                        else create_expr.this.name
                    )
                    columns: dict[str, str] = {}
                    schema = create_expr.this
                    if isinstance(schema, exp.Schema):
                        for col_def in schema.expressions:
                            if isinstance(col_def, exp.ColumnDef):
                                column_name = col_def.this.name
                                if col_def.kind:
                                    column_type = col_def.kind.sql(
                                        dialect=self.get_sqlglot_dialect()
                                    )
                                else:
                                    column_type = "UNKNOWN"
                                columns[column_name] = column_type
                        sqlglot_schema[table_name] = columns
            except Exception as e:
                logging.warning(
                    f"Could not parse DDL statement: {statement[:100]}... Error: {e}"
                )
                continue  # Move to the next statement

        return sqlglot_schema
