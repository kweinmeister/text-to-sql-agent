import logging
import re

from google.adk.agents.callback_context import CallbackContext
from google.adk.models.llm_response import LlmResponse

logger = logging.getLogger(__name__)


async def capture_user_message(callback_context: CallbackContext) -> None:
    """
    Captures the user message and saves it to state, and clears previous turn state.
    """
    # Clear state from previous turns.
    keys_to_clear = [
        "sql_query",
        "validation_result",
        "execution_result",
        "final_sql_query",
    ]
    for key in keys_to_clear:
        callback_context.state[key] = None

    user_content = callback_context.user_content
    if user_content and user_content.parts:
        full_message = " ".join(part.text for part in user_content.parts if part.text)
        callback_context.state["message"] = full_message


async def clean_sql_query(
    callback_context: CallbackContext, llm_response: LlmResponse
) -> None:
    """
    Cleans the SQL query in the LLM response by removing markdown and extracting
    only the SQL statement.
    """
    if not llm_response.content or not llm_response.content.parts:
        return

    first_part = llm_response.content.parts[0]
    if not first_part.text:
        return

    raw_text = first_part.text

    # First, try to find a SQL code block
    code_block_match = re.search(
        r"```(?:sql)?\s*(.*?)\s*```", raw_text, re.DOTALL | re.IGNORECASE
    )

    if code_block_match:
        query_text = code_block_match.group(1)
    else:
        query_text = raw_text

    # Clean up by stripping whitespace and ensuring it ends with a single semicolon
    cleaned_query = query_text.strip()
    if cleaned_query:
        words = cleaned_query.split()
        if words and words[0].isupper() and len(words[0]) > 1:
            if not cleaned_query.endswith(";"):
                cleaned_query += ";"

    if cleaned_query != raw_text:
        logger.info(f"Original LLM output: '{raw_text}'")
        logger.info(f"Cleaned SQL query: '{cleaned_query}'")
        first_part.text = cleaned_query


async def log_agent_state(callback_context: CallbackContext) -> None:
    """Logs the current state of the agent for debugging."""
    agent_name = callback_context.agent_name
    logging.info(f"--- State after {agent_name} ---")
    logging.info(callback_context.state)
    logging.info(f"--- End State for {agent_name} ---")
