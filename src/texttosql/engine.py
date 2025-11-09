import logging
from typing import Any

from sqlglot import parse_one
from sqlglot.optimizer import optimize as sqlglot_optimize
from sqlglot.schema import MappingSchema

from .dialects.dialect import DatabaseDialect

logger = logging.getLogger(__name__)


class SQLValidator:
    """Validates SQL using SQLGlot."""

    def validate(
        self,
        sql_query: str,
        dialect: DatabaseDialect,
        sqlglot_schema_dict: dict[str, dict[str, str]],
    ) -> dict[str, Any]:
        """Validates the syntax and semantics of the SQL query."""
        logger.info("--- Starting SQL Validation ---")
        logger.info(f"Query to validate: {sql_query}")
        logger.info(f"Schema being used for validation: {sqlglot_schema_dict}")
        try:
            sqlglot_dialect = dialect.get_sqlglot_dialect()
            logger.info(f"Using SQLGlot dialect: {sqlglot_dialect}")

            # 1. Parse (Syntax Check)
            logger.info("Parsing SQL query...")
            expression = parse_one(sql_query, read=sqlglot_dialect)
            logger.info(f"Parse successful. Expression type: {type(expression)}")

            # 2. Build Schema object from the provided dictionary
            logger.info("Building SQLGlot schema object...")
            schema_obj = MappingSchema(sqlglot_schema_dict, dialect=sqlglot_dialect)
            logger.info(
                f"Schema object built. Tables: {list(sqlglot_schema_dict.keys())}"
            )

            # 3. Validate
            logger.info("Running SQLGlot optimization and validation...")
            sqlglot_optimize(
                expression,
                schema=schema_obj,
                dialect=sqlglot_dialect,
                validate_qualify_columns=True,
            )
            logger.info("--- SQL Validation Successful ---")
            return {"status": "success"}

        except Exception as e:
            logger.error(f"--- SQL Validation FAILED: {e} ---", exc_info=True)
            return {"status": "error", "errors": [str(e)]}


class SQLExecutor:
    """Executes SQL queries against the database."""

    def __init__(self, db_uri: str, dialect: DatabaseDialect):
        self.db_uri = db_uri
        self.dialect = dialect

    def execute(self, sql_query: str) -> tuple[list[tuple] | None, str | None]:
        """Executes a SQL query."""
        try:
            logger.info(f"Executing SQL query: {sql_query}")
            # Delegate connection handling entirely to the dialect
            with self.dialect.get_connection(self.db_uri) as conn:
                logger.info("Database connection established")
                cursor = conn.cursor()
                logger.info("Executing query...")
                cursor.execute(sql_query)
                logger.info("Query executed successfully")
                result = cursor.fetchall()
                logger.info(f"Query returned {len(result)} rows")
                # cursor.close() and conn.close() are auto-called!
                return result, None
        except Exception as e:
            logger.error(f"SQL execution failed: {e}", exc_info=True)
            return None, str(e)
