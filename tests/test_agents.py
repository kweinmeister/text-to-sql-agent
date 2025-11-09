from texttosql.agents import CorrectionLoopAgent


def test_schema_extractor_instantiation() -> None:
    """Test that the SchemaExtractor can be instantiated."""
    from texttosql.agents import SchemaExtractor

    extractor = SchemaExtractor(name="TestExtractor")
    assert extractor is not None
    assert extractor.name == "TestExtractor"


def test_sql_processor_instantiation() -> None:
    """Test that the SQLProcessor can be instantiated."""
    from texttosql.agents import SQLProcessor

    processor = SQLProcessor(name="TestProcessor")
    assert processor is not None
    assert processor.name == "TestProcessor"


def test_sql_generator_agent_instantiation() -> None:
    """Test that the sql_generator_agent can be instantiated."""
    from texttosql.agents import sql_generator_agent

    assert sql_generator_agent is not None
    assert sql_generator_agent.name == "sql_generator_agent"


def test_sql_corrector_agent_instantiation() -> None:
    """Test that the sql_corrector_agent can be instantiated."""
    from texttosql.agents import sql_corrector_agent

    assert sql_corrector_agent is not None
    assert sql_corrector_agent.name == "sql_corrector_agent"


def test_schema_extractor_agent_instantiation() -> None:
    """Test that the schema_extractor_agent can be instantiated."""
    from texttosql.agents import schema_extractor_agent

    assert schema_extractor_agent is not None
    assert schema_extractor_agent.name == "SchemaExtractor"


def test_sql_processor_agent_instantiation() -> None:
    """Test that the sql_processor_agent can be instantiated."""
    from texttosql.agents import sql_processor_agent

    assert sql_processor_agent is not None
    assert sql_processor_agent.name == "SQLProcessor"


def test_sql_correction_loop_instantiation() -> None:
    """Test that the sql_correction_loop can be instantiated."""
    from texttosql.agents import sql_correction_loop

    assert sql_correction_loop is not None
    assert sql_correction_loop.name == "SQLCorrectionLoop"
    assert len(sql_correction_loop.sub_agents) == 2


def test_correction_loop_agent_custom_max_iterations() -> None:
    """Test that the CorrectionLoopAgent can be instantiated with custom max_iterations."""
    from texttosql.agents import SchemaExtractor, SQLProcessor

    # Create simple agents for testing
    extractor = SchemaExtractor(name="TestExtractor")
    processor = SQLProcessor(name="TestProcessor")

    loop_agent = CorrectionLoopAgent(
        name="TestLoop",
        sql_processor=extractor,
        sql_corrector=processor,
        max_iterations=5,
    )
    assert loop_agent is not None
    assert loop_agent._max_iterations == 5
