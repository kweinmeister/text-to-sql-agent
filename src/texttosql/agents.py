# agents.py
import logging
from collections.abc import AsyncGenerator
from typing import Any

from google.adk.agents import Agent, BaseAgent, LoopAgent
from google.adk.agents.invocation_context import InvocationContext
from google.adk.agents.readonly_context import ReadonlyContext
from google.adk.events import Event
from google.genai.types import Content, Part

from .callbacks import clean_sql_query
from .config import MODEL_NAME
from .dialects.factory import get_dialect
from .tools import load_schema_into_state, run_sql_execution, run_sql_validation

logger = logging.getLogger(__name__)


class SchemaExtractor(BaseAgent):
    """Agent that loads the database schema into state."""

    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[Event]:
        logger.info(f"[{self.name}] Loading schema.")
        dialect = get_dialect()
        load_schema_into_state(ctx.session.state, dialect)
        yield Event(
            author=self.name,
            invocation_id=ctx.invocation_id,
            custom_metadata={"status": "schema_loaded"},
        )


class SQLProcessor(BaseAgent):
    """
    Agent that handles the mechanical steps of:
    1. Validating the current SQL.
    2. Executing it ONLY if validation passed.
    3. Escalating to exit the loop on successful execution.
    """

    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[Event]:
        logger.info(f"[{self.name}] Starting SQL processing.")
        state = ctx.session.state
        dialect = get_dialect()

        val_result: dict[str, Any] = run_sql_validation(state, dialect)
        yield Event(
            author=self.name,
            invocation_id=ctx.invocation_id,
            custom_metadata={"validation_result": val_result},
        )

        if val_result.get("status") == "success":
            exec_result: dict[str, Any] = run_sql_execution(state, dialect)

            result_event = Event(
                author=self.name,
                invocation_id=ctx.invocation_id,
                custom_metadata={"execution_result": exec_result},
            )

            # If execution succeeds, this is the final answer.
            # Escalate to exit the loop and provide the final content.
            if exec_result.get("status") == "success":
                logger.info(
                    f"[{self.name}] SQL execution successful. Escalating to exit loop."
                )
                result_event.actions.escalate = True

                final_query: str | None = state.get("sql_query")
                state["final_sql_query"] = final_query

                if final_query:
                    result_event.content = Content(
                        role="model", parts=[Part(text=final_query)]
                    )

            yield result_event
        else:
            logger.info(f"[{self.name}] Skipping execution due to validation failure.")
            state["execution_result"] = {
                "status": "skipped",
                "reason": "validation_failed",
            }


async def get_generator_instruction(readonly_context: ReadonlyContext) -> str:
    """Dynamically builds the instruction for the SQL generator agent."""
    state = readonly_context.state

    # Safely get values from state with fallbacks
    user_question = state.get("message", "Not available")
    schema_ddl = state.get("schema_ddl", "Not available")

    return f"""You are an expert SQL writer. Based on the user's question and the provided database schema, write a single, syntactically correct SQL query to answer the question.

Rules:
1. Respond ONLY with the SQL query. Do not add any markdown formatting.
2. **Schema is Truth:** USE ONLY TABLES AND COLUMNS LISTED IN THE DATABASE SCHEMA below. Do not assume or hallucinate table names (e.g., if the schema says 'film', do NOT use 'films').

User Question:
{user_question}
Database Schema:
{schema_ddl}
"""


sql_generator_agent = Agent(
    name="sql_generator_agent",
    model=MODEL_NAME,
    description="Generates an initial SQL query from a natural language question.",
    instruction=get_generator_instruction,
    output_key="sql_query",
    after_model_callback=clean_sql_query,
)


async def get_corrector_instruction(readonly_context: ReadonlyContext) -> str:
    """Dynamically builds the instruction for the SQL corrector agent."""
    state = readonly_context.state

    # Safely get values from state with fallbacks
    user_question = state.get("message", "Not available")
    faulty_query = state.get("sql_query", "Not available")
    schema_ddl = state.get("schema_ddl", "Not available")
    validation_result = state.get("validation_result", "Not available")
    execution_result = state.get("execution_result", "Not available")

    return f"""You are a SQL expert tasked with correcting a failed SQL query.

The previous attempt failed. Use the following information to fix the query:
- Original User Question: {user_question}
- Faulty SQL Query: {faulty_query}
- Database Schema (Source of Truth): {schema_ddl}
- Validation Errors: {validation_result}
- Execution Error: {execution_result}

**Correction Rules:**
1.  **Prioritize the Execution Error:** The `Execution Error` comes directly from the database and is the most reliable source of truth. If it reports an error (e.g., "no such table"), you MUST correct the query to fix it.
2.  **Adhere Strictly to the Schema:** Use the `Database Schema` to find the correct table and column names. Do not infer or guess names (e.g., if the schema has `customer`, you must use `customer`, not `customers`).
3.  **Respond ONLY with the corrected, single SQL query. Do not add markdown or explanations.
"""


sql_corrector_agent = Agent(
    name="sql_corrector_agent",
    model=MODEL_NAME,
    description="Corrects a failed SQL query.",
    instruction=get_corrector_instruction,
    output_key="sql_query",
    tools=[],
    after_model_callback=clean_sql_query,
)

schema_extractor_agent = SchemaExtractor(name="SchemaExtractor")

sql_processor_agent = SQLProcessor(name="SQLProcessor")


sql_correction_loop = LoopAgent(
    name="SQLCorrectionLoop",
    sub_agents=[
        sql_processor_agent,
        sql_corrector_agent,
    ],
    max_iterations=3,
)
