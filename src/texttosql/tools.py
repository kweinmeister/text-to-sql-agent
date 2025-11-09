import logging
import os
from typing import Any

from .config import DB_URI
from .dialects.dialect import DatabaseDialect

logger = logging.getLogger(__name__)


def load_schema_into_state(state: dict[str, Any], dialect: DatabaseDialect) -> None:
    """
    Loads the DDL and SQLGlot schema into the state dictionary.
    This function relies on the caching mechanism within the dialect object.
    """
    logger.info(f"Loading schema for dialect: {dialect.name}")

    db_uri = DB_URI
    if not db_uri:
        error_msg = "Error: DB_URI environment variable not set."
        logger.error(error_msg)
        state["schema_ddl"] = f"Error loading schema: {error_msg}"
        state["sqlglot_schema"] = {"error": error_msg}
        return

    try:
        logger.info(f"Loading schema from database: {db_uri}")
        # The dialect object handles its own caching.
        # The first call to get_ddl will trigger the DB query and cache the DDL.
        logger.info("Calling dialect.get_ddl...")
        state["schema_ddl"] = dialect.get_ddl(db_uri)
        logger.info("DDL loaded successfully")

        # The call to get_sqlglot_schema will use the cached DDL if available,
        # then parse it and cache the result.
        logger.info("Calling dialect.get_sqlglot_schema...")
        state["sqlglot_schema"] = dialect.get_sqlglot_schema(db_uri)
        logger.info("SQLGlot schema loaded successfully")
        logger.info(f"SQLGlot schema keys: {list(state['sqlglot_schema'].keys())}")

    except Exception as e:
        error_msg = f"Error extracting schema: {e}"
        logger.error(error_msg, exc_info=True)
        state["schema_ddl"] = f"Error loading schema: {error_msg}"
        state["sqlglot_schema"] = {"error": error_msg}


def run_sql_validation(
    state: dict[str, Any], dialect: DatabaseDialect
) -> dict[str, Any]:
    """Validates the SQL query currently in state."""
    sql_query = state.get("sql_query")
    logger.info("Validating SQL: %s", sql_query)

    from .engine import SQLValidator

    validator = SQLValidator()
    # Use the 'sqlglot_schema' key from the state, which we populated earlier.
    sqlglot_schema = state.get("sqlglot_schema")
    logger.info(
        f"SQLGlot schema keys: {list(sqlglot_schema.keys()) if sqlglot_schema else None}"
    )

    if not sql_query:
        logger.warning("No SQL query found in state to validate.")
        return {
            "status": "error",
            "errors": ["No SQL query found in state to validate."],
        }

    if not sqlglot_schema or "error" in sqlglot_schema:
        # Provide more specific troubleshooting information
        error_details = []
        if not sqlglot_schema:
            error_details.append("SQLGlot schema is None or empty")
        elif "error" in sqlglot_schema:
            error_details.append(
                f"SQLGlot schema contains error: {sqlglot_schema['error']}"
            )

        # Check if DB_URI is set
        if not DB_URI:
            error_details.append("DB_URI environment variable is not set")
        else:
            error_details.append(f"DB_URI is set to: {DB_URI}")

        # For file-based databases (like SQLite), check if the database file exists and is not empty
        # We'll assume it's file-based if it doesn't look like a URL (contains ://)
        if DB_URI and "://" not in DB_URI:
            if not os.path.exists(DB_URI):
                error_details.append(f"Database file does not exist: {DB_URI}")
            elif os.path.getsize(DB_URI) == 0:
                error_details.append(f"Database file is empty: {DB_URI}")
        elif DB_URI and "://" in DB_URI:
            error_details.append(
                "Ensure the database server is running and accessible."
            )

        logger.warning(
            "Database schema not available for validation. Details: %s",
            "; ".join(error_details),
        )
        return {
            "status": "error",
            "errors": ["Database schema not available for validation.", *error_details],
        }

    # Pass the correct schema dictionary to the validator.
    logger.info("Calling SQLValidator.validate...")
    validation_result = validator.validate(sql_query, dialect, sqlglot_schema)
    state["validation_result"] = validation_result
    logger.info(f"Validation result: {validation_result}")
    return validation_result


def run_sql_execution(
    state: dict[str, Any], dialect: DatabaseDialect
) -> dict[str, Any]:
    """Executes the SQL query currently in state."""
    sql_query = state.get("sql_query")
    logger.info("Executing SQL: %s", sql_query)

    db_uri = DB_URI
    if not db_uri:
        logger.error("DB_URI not set.")
        return {"status": "error", "error_message": "DB_URI not set."}

    if not sql_query:
        logger.warning("No SQL query found in state to execute.")
        result = {
            "status": "error",
            "error_message": "No SQL query found in state to execute.",
        }
        state["execution_result"] = result
        return result

    from .engine import SQLExecutor

    executor = SQLExecutor(db_uri, dialect)
    logger.info("Calling SQLExecutor.execute...")
    result_data, error = executor.execute(sql_query)

    execution_result: dict[str, Any] = {}
    if error:
        logger.error(f"SQL execution failed: {error}")
        execution_result["status"] = "error"
        execution_result["error_message"] = error
    else:
        logger.info("SQL execution successful")
        execution_result["status"] = "success"
        execution_result["result"] = result_data

    state["execution_result"] = execution_result
    return execution_result
