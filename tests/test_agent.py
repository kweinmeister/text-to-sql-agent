import importlib
import logging
import os
import sqlite3
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


@pytest.fixture
def temp_sqlite_db(tmp_path) -> str:
    """Create a temporary SQLite database for testing."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # A simple schema to test against
    cursor.execute("""
    CREATE TABLE customer (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT
    );
    """)
    # Insert some test data
    cursor.execute("INSERT INTO customer (id, name) VALUES (1, 'John Doe');")
    cursor.execute("INSERT INTO customer (id, name) VALUES (2, 'Jane Smith');")
    conn.commit()
    conn.close()
    return str(db_path)


@pytest.fixture(scope="module", autouse=True)
def load_env() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    from dotenv import load_dotenv

    load_dotenv("src/texttosql/.env")


@pytest.mark.asyncio
@patch("google.adk.models.google_llm.Gemini.generate_content_async")
async def test_agent_run_success(mock_generate_content_async, temp_sqlite_db) -> None:
    """Tests a successful run of the agent from question to final SQL."""

    # Configure the mock to return a valid, simple SQL query.
    async def mock_async_generator(*args, **kwargs):
        yield LlmResponse(
            content=types.Content(
                parts=[types.Part(text="SELECT COUNT(*) FROM customer;")]
            )
        )

    mock_generate_content_async.return_value = mock_async_generator()

    # Temporarily override the DB_URI with our test database
    original_db_uri = os.environ.get("DB_URI")
    os.environ["DB_URI"] = temp_sqlite_db

    # Re-import the modules to pick up the new DB_URI
    import texttosql.config
    import texttosql.dialects.factory
    import texttosql.tools

    importlib.reload(texttosql.config)
    importlib.reload(texttosql.tools)
    importlib.reload(texttosql.dialects.factory)

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

    # Restore the original DB_URI
    if original_db_uri is not None:
        os.environ["DB_URI"] = original_db_uri
    elif "DB_URI" in os.environ:
        del os.environ["DB_URI"]

    # Re-import the modules to restore the original DB_URI
    importlib.reload(texttosql.config)
    importlib.reload(texttosql.tools)
    importlib.reload(texttosql.dialects.factory)

    assert len(events) > 0, "Expected at least one message"

    # Check for the final output from the mock
    final_event = events[-1]
    assert final_event.content is not None, "Final event content should not be None"
    assert final_event.content.parts is not None
    final_text = "".join(part.text for part in final_event.content.parts if part.text)
    assert "SELECT COUNT(*) FROM customer;" in final_text
