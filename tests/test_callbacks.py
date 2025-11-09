from unittest.mock import MagicMock

import pytest
from google.genai.types import Content, Part

from texttosql.callbacks import capture_user_message, clean_sql_query, log_agent_state


@pytest.mark.asyncio
async def test_capture_user_message():
    """Test that the user message is captured and old state is cleared."""
    mock_context = MagicMock()
    mock_context.state = {
        "sql_query": "old_query",
        "validation_result": {"status": "success"},
        "execution_result": {"status": "success"},
        "final_sql_query": "old_query",
        "message": "old_message",
    }
    mock_context.user_content = Content(role="user", parts=[Part(text="Hello world")])

    await capture_user_message(mock_context)

    assert mock_context.state["message"] == "Hello world"
    assert mock_context.state["sql_query"] is None
    assert mock_context.state["validation_result"] is None
    assert mock_context.state["execution_result"] is None
    assert mock_context.state["final_sql_query"] is None


@pytest.mark.asyncio
async def test_capture_user_message_with_empty_content():
    """Test capture_user_message when user_content is None."""
    mock_context = MagicMock()
    mock_context.state = {"sql_query": "old_query", "message": "old_message"}
    mock_context.user_content = None

    await capture_user_message(mock_context)

    # State should still be cleared even with empty content
    # But message should remain unchanged since there's no content to set
    assert mock_context.state["sql_query"] is None
    assert mock_context.state["message"] == "old_message"


@pytest.mark.asyncio
async def test_capture_user_message_with_empty_parts():
    """Test capture_user_message when user_content.parts is empty."""
    mock_context = MagicMock()
    mock_context.state = {"sql_query": "old_query", "message": "old_message"}
    mock_context.user_content = Content(role="user", parts=[])

    await capture_user_message(mock_context)

    # State should still be cleared even with empty parts
    # But message should remain unchanged since there are no parts to process
    assert mock_context.state["sql_query"] is None
    assert mock_context.state["message"] == "old_message"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw_text, expected_query",
    [
        ("SELECT * FROM film;", "SELECT * FROM film;"),
        ("```sql\nSELECT * FROM film;\n```", "SELECT * FROM film;"),
        (
            "Here is your query:\n```\nSELECT * FROM film;\n```\nEnjoy!",
            "SELECT * FROM film;",
        ),
        ("   SELECT * FROM film;   ", "SELECT * FROM film;"),
        ("SELECT * FROM film;;;", "SELECT * FROM film;"),
        ("This is not SQL.", "This is not SQL.;"),  # Edge case: no SELECT
    ],
)
async def test_clean_sql_query(raw_text, expected_query):
    """Test clean_sql_query with various LLM response formats."""
    mock_response = MagicMock()
    mock_response.content = Content(role="model", parts=[Part(text=raw_text)])
    mock_context = MagicMock()

    await clean_sql_query(mock_context, mock_response)

    assert mock_response.content.parts[0].text == expected_query


@pytest.mark.asyncio
async def test_clean_sql_query_with_no_content():
    """Test clean_sql_query when there is no content."""
    mock_response = MagicMock()
    mock_response.content = None
    mock_context = MagicMock()

    # Should not raise an exception
    await clean_sql_query(mock_context, mock_response)


@pytest.mark.asyncio
async def test_clean_sql_query_with_no_parts():
    """Test clean_sql_query when there are no parts."""
    mock_response = MagicMock()
    mock_response.content = Content(role="model", parts=[])
    mock_context = MagicMock()

    # Should not raise an exception
    await clean_sql_query(mock_context, mock_response)


@pytest.mark.asyncio
async def test_clean_sql_query_with_no_text():
    """Test clean_sql_query when the part has no text."""
    mock_response = MagicMock()
    mock_response.content = Content(role="model", parts=[Part(text=None)])
    mock_context = MagicMock()

    # Should not raise an exception
    await clean_sql_query(mock_context, mock_response)
    # Text should remain None
    assert mock_response.content.parts[0].text is None


@pytest.mark.asyncio
async def test_clean_sql_query_with_empty_text():
    """Test clean_sql_query when the part has empty text."""
    mock_response = MagicMock()
    mock_response.content = Content(role="model", parts=[Part(text="")])
    mock_context = MagicMock()

    # Should not raise an exception
    await clean_sql_query(mock_context, mock_response)

    # Text should remain empty since empty string is not considered a valid query
    assert mock_response.content.parts[0].text == ""


@pytest.mark.asyncio
async def test_log_agent_state():
    """Test the log_agent_state callback."""
    mock_context = MagicMock()
    mock_context.agent_name = "test_agent"
    mock_context.state = {"key": "value"}

    # Should not raise an exception
    await log_agent_state(mock_context)
