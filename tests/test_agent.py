import logging
from unittest.mock import patch

import pytest
from google.adk.agents.run_config import RunConfig, StreamingMode
from google.adk.events import Event
from google.adk.models.llm_response import LlmResponse
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types
from google.genai.types import Content, Part

from texttosql.agent import root_agent
from texttosql.config import DB_URI


@pytest.fixture(scope="module", autouse=True)
def load_env() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    from dotenv import load_dotenv

    load_dotenv("src/texttosql/.env")

    db_uri = DB_URI
    if not db_uri:
        raise RuntimeError(
            "DB_URI environment variable not set. Please set DB_URI in .env"
        )


@pytest.mark.asyncio
@patch("texttosql.tools.load_schema_into_state")
@patch("google.adk.models.google_llm.Gemini.generate_content_async")
async def test_agent_run_success(mock_generate_content_async, mock_load_schema_into_state) -> None:
    """Tests a successful run of the agent from question to final SQL."""

    # Mock the schema loading to provide a simple schema
    mock_load_schema_into_state.side_effect = lambda state, dialect: state.update({
        "schema_ddl": "CREATE TABLE customer (id INTEGER PRIMARY KEY, name TEXT);",
        "sqlglot_schema": {"customer": {"id": "INTEGER", "name": "TEXT"}}
    })

    # Configure the mock to return a valid, simple SQL query.
    async def mock_async_generator(*args, **kwargs):
        yield LlmResponse(
            content=types.Content(
                parts=[types.Part(text="SELECT COUNT(*) FROM customer;")]
            )
        )

    mock_generate_content_async.return_value = mock_async_generator()

    session_service = InMemorySessionService()
    session = await session_service.create_session(
        user_id="test_user", app_name="texttosql"
    )
    runner = Runner(
        agent=root_agent, session_service=session_service, app_name="texttosql"
    )

    message = Content(
        role="user",
        parts=[Part.from_text(text="how many customers are there")],
    )

    events: list[Event] = []
    async for event in runner.run_async(
        new_message=message,
        user_id="test_user",
        session_id=session.id,
        run_config=RunConfig(streaming_mode=StreamingMode.SSE),
    ):
        events.append(event)

    assert len(events) > 0, "Expected at least one message"

    # Check for the final output from the mock
    final_event = events[-1]
    assert final_event.content is not None, "Final event content should not be None"
    assert final_event.content.parts is not None
    final_text = "".join(part.text for part in final_event.content.parts if part.text)
    assert "SELECT COUNT(*) FROM customer;" in final_text
