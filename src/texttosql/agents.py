# agents.py
import logging
from collections.abc import AsyncGenerator
from typing import Any

from google.adk.agents import Agent, BaseAgent
from google.adk.agents.invocation_context import InvocationContext
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
            yield Event(
                author=self.name,
                invocation_id=ctx.invocation_id,
                custom_metadata={"execution_result": exec_result},
            )
        else:
            logger.info(f"[{self.name}] Skipping execution due to validation failure.")
            state["execution_result"] = {
                "status": "skipped",
                "reason": "validation_failed",
            }


sql_generator_agent = Agent(
    name="sql_generator_agent",
    model=MODEL_NAME,
    description="Generates an initial SQL query from a natural language question.",
    instruction="""You are an expert SQL writer. Based on the user's question and the provided database schema, write a single, syntactically correct SQL query to answer the question.

Rules:
1. Respond ONLY with the SQL query. Do not add any markdown formatting.
2. **Schema is Truth:** USE ONLY TABLES AND COLUMNS LISTED IN THE DATABASE SCHEMA below. Do not assume or hallucinate table names (e.g., if the schema says 'film', do NOT use 'films').

User Question:
{state.message}
Database Schema:
{state.schema_ddl}
""",
    output_key="sql_query",
    after_model_callback=clean_sql_query,
)

sql_corrector_agent = Agent(
    name="sql_corrector_agent",
    model=MODEL_NAME,
    description="Corrects a failed SQL query.",
    instruction="""You are a SQL expert tasked with correcting a failed SQL query.

The previous attempt failed. Use the following information to fix the query:
- Original User Question: {state.message}
- Faulty SQL Query: {state.sql_query}
- Database Schema (Source of Truth): {state.schema_ddl}
- Validation Errors: {state.validation_result}
- Execution Error: {state.execution_result}

**Correction Rules:**
1. Respond ONLY with the corrected, single SQL query. Do not add markdown or explanations.
2. Use the exact table and column names from the schema.
3. Fix the query to answer the original user question.

Corrected SQL Query:
""",
    output_key="sql_query",
    tools=[],
    after_model_callback=clean_sql_query,
)

schema_extractor_agent = SchemaExtractor(name="SchemaExtractor")

sql_processor_agent = SQLProcessor(name="SQLProcessor")


class CorrectionLoopAgent(BaseAgent):
    """A loop agent for SQL correction with a clean exit condition."""

    def __init__(
        self,
        name: str,
        sql_processor: BaseAgent,
        sql_corrector: BaseAgent,
        max_iterations: int = 3,
    ) -> None:
        super().__init__(name=name, sub_agents=[sql_processor, sql_corrector])
        self._sql_processor = sql_processor
        self._sql_corrector = sql_corrector
        self._max_iterations = max_iterations

    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[Event]:
        # Use the explicitly named agents
        sql_processor = self._sql_processor
        sql_corrector = self._sql_corrector

        for i in range(self._max_iterations):
            logger.info(f"[{self.name}] Starting correction loop iteration {i + 1}.")
            logger.info(
                f"[{self.name}] Current SQL query in state: {ctx.session.state.get('sql_query')}"
            )

            # --- Step 1: Process (Validate & Execute) ---
            #
            # Emit a custom event to signal the start of the sub-agent run
            yield Event(
                author=self.name,
                invocation_id=ctx.invocation_id,
                custom_metadata={"invoking_sub_agent": sql_processor.name},
            )
            async for event in sql_processor.run_async(ctx):
                yield event

            # Step 2: Check the result deterministically
            execution_result: dict[str, Any] = ctx.session.state.get(
                "execution_result", {}
            )
            if execution_result.get("status") == "success":
                logger.info(f"[{self.name}] SQL execution successful. Exiting loop.")

                final_query: str | None = ctx.session.state.get("sql_query")
                ctx.session.state["final_sql_query"] = final_query

                if final_query:
                    yield Event(
                        author=self.name,
                        invocation_id=ctx.invocation_id,
                        content=Content(role="model", parts=[Part(text=final_query)]),
                    )
                return

            logger.info(f"[{self.name}] SQL failed. Invoking corrector agent.")

            # --- Step 3: If not successful, invoke the corrector ---
            #
            # Emit another custom event to signal the start of the next sub-agent run
            yield Event(
                author=self.name,
                invocation_id=ctx.invocation_id,
                custom_metadata={"invoking_sub_agent": sql_corrector.name},
            )
            async for event in sql_corrector.run_async(ctx):
                yield event

        logger.warning(
            f"[{self.name}] Max iterations reached without a successful query."
        )

        # Get the last attempted query and error message
        last_query = ctx.session.state.get("sql_query", "Unknown")
        execution_result = ctx.session.state.get("execution_result", {})
        error_message = execution_result.get("error_message", "Unknown error")

        # Yield a structured error event
        yield Event(
            author=self.name,
            invocation_id=ctx.invocation_id,
            custom_metadata={
                "status": "error",
                "error_type": "max_iterations_reached",
                "last_query": last_query,
                "final_error": error_message,
            },
        )


sql_correction_loop = CorrectionLoopAgent(
    name="SQLCorrectionLoop",
    sql_processor=sql_processor_agent,
    sql_corrector=sql_corrector_agent,
    max_iterations=3,
)
