import pytest

from alembic_utils.exceptions import SQLParseFailure
from alembic_utils.pg_view import PGView


def test_unparsable_view() -> None:
    SQL = "create or replace vew public.some_view as select 1 one;"
    with pytest.raises(SQLParseFailure):
        view = PGView.from_sql(SQL)


def test_parsable_body() -> None:
    SQL = "create or replace view public.some_view as select 1 one;"
    try:
        view = PGView.from_sql(SQL)
    except SQLParseFailure:
        pytest.fail(f"Unexpected SQLParseFailure for view {SQL}")


def test_teardown_temp_schema_on_error(engine, reset) -> None:
    """Make sure the temporary schema gets town down when the simulated entity fails"""
    SQL = "create or replace view public.some_view as INVALID SQL!;"
    view = PGView.from_sql(SQL)

    with engine.connect() as connection:
        with pytest.raises(Exception):
            view.simulate_database_entity(connection)

        maybe_schema = connection.execute(
            "select * from pg_namespace where nspname = 'alembic_utils';"
        ).fetchone()
        assert maybe_schema is None
