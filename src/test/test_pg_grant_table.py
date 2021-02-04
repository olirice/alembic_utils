import pytest

from alembic_utils.pg_grant_table import GrantOption, PGGrantTable
from alembic_utils.replaceable_entity import register_entities
from alembic_utils.testbase import TEST_VERSIONS_ROOT, run_alembic_command


@pytest.fixture(scope="function")
def sql_setup(engine):
    conn = engine
    conn.execute(
        """
    create table public.account (
        id serial primary key,
        email text not null
    );
    create role anon_user
    """
    )

    yield
    conn.execute("drop table public.account cascade")


DEFAULT_GRANT = PGGrantTable(
    schema="public", table="account", role="alem_user", grant_options=["ALL"]
)


TEST_GRANT = PGGrantTable(
    schema="public", table="account", role="anon_user", grant_options=["SELECT", "UPDATE"]
)


def test_repr():
    go = GrantOption("SELECT")
    assert go.__str__() == go.__repr__()


def test_create_revision(sql_setup, engine) -> None:
    register_entities([TEST_GRANT, DEFAULT_GRANT], entity_types=[PGGrantTable])
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
    assert "from alembic_utils.pg_grant_table import PGGrantTable" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_update_revision(sql_setup, engine) -> None:
    engine.execute(TEST_GRANT.to_sql_statement_create())

    UPDATED_GRANT = PGGrantTable(
        schema="public", table="account", role="anon_user", grant_options=["SELECT"]
    )

    register_entities([UPDATED_GRANT, DEFAULT_GRANT], entity_types=[PGGrantTable])
    run_alembic_command(
        engine=engine,
        command="revision",
        command_kwargs={"autogenerate": True, "rev_id": "2", "message": "update"},
    )

    migration_create_path = TEST_VERSIONS_ROOT / "2_update.py"

    with migration_create_path.open() as migration_file:
        migration_contents = migration_file.read()

    assert "op.replace_entity" in migration_contents
    assert "op.create_entity" not in migration_contents
    assert "op.drop_entity" not in migration_contents
    assert "from alembic_utils.pg_grant_table import PGGrantTable" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})
