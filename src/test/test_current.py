from alembic_utils.testbase import run_alembic_command


def test_current(engine) -> None:
    """Test that the alembic current command does not erorr"""
    # Runs with no error
    output = run_alembic_command(engine, "current", {})
    assert output == ""
