import logging
import os
import sys

# Add the src directory to the Python path to ensure correct module resolution
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))


import pytest
from google.adk.agents.run_config import RunConfig, StreamingMode
from google.adk.events import Event
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
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
async def test_agent_run_success() -> None:
    """Tests a successful run of the agent from question to final SQL."""
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

    # Check for the final output
    final_event = events[-1]
    assert final_event.content is not None, "Final event content should not be None"
    assert final_event.content.parts is not None
    assert any(
        "SELECT" in part.text
        for part in final_event.content.parts
        if part.text is not None
    ), "Expected final event to contain a SQL query"
