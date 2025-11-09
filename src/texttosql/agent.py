# texttosql/agent.py

from google.adk.agents import SequentialAgent

from .agents import (
    schema_extractor_agent,
    sql_correction_loop,
    sql_generator_agent,
)
from .callbacks import capture_user_message

root_agent = SequentialAgent(
    name="TextToSqlRootAgent",
    before_agent_callback=capture_user_message,
    sub_agents=[
        schema_extractor_agent,
        sql_generator_agent,
        sql_correction_loop,
    ],
)
