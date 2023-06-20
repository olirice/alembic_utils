from sqlalchemy import text

from alembic_utils.pg_function import PGFunction
from alembic_utils.replaceable_entity import register_entities
from alembic_utils.testbase import TEST_VERSIONS_ROOT, run_alembic_command

TO_UPPER = PGFunction(
    schema="public",
    signature="toUpper(some_text text default 'my text!')",
    definition="""
        returns text
        as
        $$ begin return upper(some_text) || 'abc'; end; $$ language PLPGSQL;
        """,
)


def test_issue_110(engine) -> None:
    register_entities([TO_UPPER], entity_types=[PGFunction])

    with engine.connect() as con:
        con.execute(
            text(
                """
        CREATE FUNCTION time_subtype_diff(x time, y time) RETURNS float8 AS
            'SELECT EXTRACT(EPOCH FROM (x - y))' LANGUAGE sql STRICT IMMUTABLE;

        CREATE TYPE timerange AS RANGE (
            subtype = time,
            subtype_diff = time_subtype_diff
        )
        """
            )
        )

    run_alembic_command(
        engine=engine,
        command="revision",
        command_kwargs={"autogenerate": True, "rev_id": "1", "message": "create"},
    )

    migration_create_path = TEST_VERSIONS_ROOT / "1_create.py"

    with migration_create_path.open() as migration_file:
        migration_contents = migration_file.read()

    assert "op.create_entity" in migration_contents
    assert "op.drop_entity" in migration_contents
    assert "op.replace_entity" not in migration_contents
    assert "from alembic_utils.pg_function import PGFunction" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})
