import logging
from typing import Any

from .config import DB_URI
from .dialects.dialect import DatabaseDialect


def load_schema_into_state(state: dict[str, Any], dialect: DatabaseDialect) -> None:
    """
    Loads the DDL and SQLGlot schema into the state dictionary.
    This function relies on the caching mechanism within the dialect object.
    """
    logging.info(f"Loading schema for dialect: {dialect.name}")

    db_uri = DB_URI
    if not db_uri:
        error_msg = "Error: DB_URI environment variable not set."
        logging.error(error_msg)
        state["schema_ddl"] = f"Error loading schema: {error_msg}"
        state["sqlglot_schema"] = {"error": error_msg}
        return

    try:
        logging.info(f"Loading schema from database: {db_uri}")
        # The dialect object handles its own caching.
        # The first call to get_ddl will trigger the DB query and cache the DDL.
        logging.info("Calling dialect.get_ddl...")
        state["schema_ddl"] = dialect.get_ddl(db_uri)
        logging.info("DDL loaded successfully")

        # The call to get_sqlglot_schema will use the cached DDL if available,
        # then parse it and cache the result.
        logging.info("Calling dialect.get_sqlglot_schema...")
        state["sqlglot_schema"] = dialect.get_sqlglot_schema(db_uri)
        logging.info("SQLGlot schema loaded successfully")
        logging.info(f"SQLGlot schema keys: {list(state['sqlglot_schema'].keys())}")

    except Exception as e:
        error_msg = f"Error extracting schema: {e}"
        logging.error(error_msg, exc_info=True)
        state["schema_ddl"] = f"Error loading schema: {error_msg}"
        state["sqlglot_schema"] = {"error": error_msg}


def run_sql_validation(
    state: dict[str, Any], dialect: DatabaseDialect
) -> dict[str, Any]:
    """Validates the SQL query currently in state."""
    sql_query = state.get("sql_query")
    logging.info("Validating SQL: %s", sql_query)

    from .engine import SQLValidator

    validator = SQLValidator()
    # Use the 'sqlglot_schema' key from the state, which we populated earlier.
    sqlglot_schema = state.get("sqlglot_schema")
    logging.info(
        f"SQLGlot schema keys: {list(sqlglot_schema.keys()) if sqlglot_schema else None}"
    )

    if not sql_query:
        logging.warning("No SQL query found in state to validate.")
        return {
            "status": "error",
            "errors": ["No SQL query found in state to validate."],
        }

    if not sqlglot_schema or "error" in sqlglot_schema:
        logging.warning("Database schema not available for validation.")
        return {
            "status": "error",
            "errors": ["Database schema not available for validation."],
        }

    # Pass the correct schema dictionary to the validator.
    logging.info("Calling SQLValidator.validate...")
    validation_result = validator.validate(sql_query, dialect, sqlglot_schema)
    state["validation_result"] = validation_result
    logging.info(f"Validation result: {validation_result}")
    return validation_result


def run_sql_execution(
    state: dict[str, Any], dialect: DatabaseDialect
) -> dict[str, Any]:
    """Executes the SQL query currently in state."""
    sql_query = state.get("sql_query")
    logging.info("Executing SQL: %s", sql_query)

    db_uri = DB_URI
    if not db_uri:
        logging.error("DB_URI not set.")
        return {"status": "error", "error_message": "DB_URI not set."}

    if not sql_query:
        logging.warning("No SQL query found in state to execute.")
        result = {
            "status": "error",
            "error_message": "No SQL query found in state to execute.",
        }
        state["execution_result"] = result
        return result

    from .engine import SQLExecutor

    executor = SQLExecutor(db_uri, dialect)
    logging.info("Calling SQLExecutor.execute...")
    result_data, error = executor.execute(sql_query)

    execution_result: dict[str, Any] = {}
    if error:
        logging.error(f"SQL execution failed: {error}")
        execution_result["status"] = "error"
        execution_result["error_message"] = error
    else:
        logging.info("SQL execution successful")
        execution_result["status"] = "success"
        execution_result["result"] = result_data

    state["execution_result"] = execution_result
    return execution_result
