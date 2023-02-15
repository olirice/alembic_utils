import pytest
from sqlalchemy import text
from alembic_utils.pg_grant_table import PGGrantTable, PGGrantTableChoice
from alembic_utils.replaceable_entity import register_entities
from alembic_utils.testbase import TEST_VERSIONS_ROOT, run_alembic_command

# TODO
"""
- Cant revoke permissions from superuser so they need to be filtered out
- Multiple users may grant the same permsision to a role so we need a new
    parameter for grantor and a check that "CURENT_USER == grantor"
"""


@pytest.fixture(scope="function")
def sql_setup(engine):
    with engine.begin() as connection:
        connection.execute(text(
            """
        create table public.account (
            id serial primary key,
            email text not null
        );
        create role anon_user
        """
        ))

    yield
    with engine.begin() as connection:
        connection.execute(text("drop table public.account cascade"))


TEST_GRANT = PGGrantTable(
    schema="public",
    table="account",
    columns=["id", "email"],
    role="anon_user",
    grant=PGGrantTableChoice.SELECT,
    with_grant_option=False,
)


def test_repr():
    go = PGGrantTableChoice("SELECT")
    assert go.__repr__() == "'SELECT'"


def test_create_revision(sql_setup, engine) -> None:
    register_entities([TEST_GRANT], entity_types=[PGGrantTable])
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


def test_replace_revision(sql_setup, engine) -> None:
    with engine.begin() as connection:
        connection.execute(TEST_GRANT.to_sql_statement_create())

    UPDATED_GRANT = PGGrantTable(
        schema="public",
        table="account",
        columns=["id"],
        role="anon_user",
        grant=PGGrantTableChoice.SELECT,
        with_grant_option=True,
    )

    register_entities([UPDATED_GRANT], entity_types=[PGGrantTable])
    run_alembic_command(
        engine=engine,
        command="revision",
        command_kwargs={"autogenerate": True, "rev_id": "2", "message": "update"},
    )

    migration_create_path = TEST_VERSIONS_ROOT / "2_update.py"

    with migration_create_path.open() as migration_file:
        migration_contents = migration_file.read()

    # Granting can not be done in place.
    assert "op.replace_entity" in migration_contents
    assert "op.create_entity" not in migration_contents
    assert "op.drop_entity" not in migration_contents
    assert "from alembic_utils.pg_grant_table import PGGrantTable" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_noop_revision(sql_setup, engine) -> None:
    # Create the view outside of a revision
    with engine.begin() as connection:
        connection.execute(TEST_GRANT.to_sql_statement_create())

    register_entities([TEST_GRANT], entity_types=[PGGrantTable])

    # Create a third migration without making changes.
    # This should result in no create, drop or replace statements
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})

    output = run_alembic_command(
        engine=engine,
        command="revision",
        command_kwargs={"autogenerate": True, "rev_id": "3", "message": "do_nothing"},
    )
    migration_do_nothing_path = TEST_VERSIONS_ROOT / "3_do_nothing.py"

    with migration_do_nothing_path.open() as migration_file:
        migration_contents = migration_file.read()

    assert "op.create_entity" not in migration_contents
    assert "op.drop_entity" not in migration_contents
    assert "op.replace_entity" not in migration_contents
    assert "from alembic_utils" not in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_drop_revision(sql_setup, engine) -> None:

    # Register no functions locally
    register_entities([], schemas=["public"], entity_types=[PGGrantTable])

    # Manually create a SQL function
    with engine.begin() as connection:
        connection.execute(TEST_GRANT.to_sql_statement_create())

    output = run_alembic_command(
        engine=engine,
        command="revision",
        command_kwargs={"autogenerate": True, "rev_id": "1", "message": "drop"},
    )

    migration_create_path = TEST_VERSIONS_ROOT / "1_drop.py"

    with migration_create_path.open() as migration_file:
        migration_contents = migration_file.read()

    # import pdb; pdb.set_trace()

    assert "op.drop_entity" in migration_contents
    assert "op.create_entity" in migration_contents
    assert "from alembic_utils" in migration_contents
    assert migration_contents.index("op.drop_entity") < migration_contents.index("op.create_entity")

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})
