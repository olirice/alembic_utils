from typing import Generator

import pytest
from sqlalchemy import text

from alembic_utils.exceptions import SQLParseFailure
from alembic_utils.pg_policy import PGPolicy
from alembic_utils.replaceable_entity import register_entities
from alembic_utils.testbase import TEST_VERSIONS_ROOT, run_alembic_command

TEST_POLICY = PGPolicy(
    schema="public",
    signature="some_policy",
    on_entity="some_tab",  # schema omitted intentionally
    definition="""
    for all
    to anon_user
    using (true)
    with check (true);
    """,
)


@pytest.fixture()
def schema_setup(engine) -> Generator[None, None, None]:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
        create table public.some_tab (
            id serial primary key,
            name text
        );

        create user anon_user;
        """
            )
        )
    yield
    with engine.begin() as connection:
        connection.execute(
            text(
                """
        drop table public.some_tab;
        drop user anon_user;
        """
            )
        )


def test_unparsable() -> None:
    sql = "create po mypol on public.sometab for all;"
    with pytest.raises(SQLParseFailure):
        PGPolicy.from_sql(sql)


def test_parse_without_schema_on_entity() -> None:

    sql = "CREATE POLICY mypol on accOunt as PERMISSIVE for SELECT to account_creator using (true) wiTh CHECK (true)"

    policy = PGPolicy.from_sql(sql)
    assert policy.schema == "public"
    assert policy.signature == "mypol"
    assert policy.on_entity == "accOunt"
    assert (
        policy.definition
        == "as PERMISSIVE for SELECT to account_creator using (true) wiTh CHECK (true)"
    )


def test_create_revision(engine, schema_setup) -> None:
    register_entities([TEST_POLICY], entity_types=[PGPolicy])

    output = run_alembic_command(
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
    assert "from alembic_utils.pg_policy import PGPolicy" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_update_revision(engine, schema_setup) -> None:
    # Create the view outside of a revision
    with engine.begin() as connection:
        connection.execute(TEST_POLICY.to_sql_statement_create())

    # Update definition of TO_UPPER
    UPDATED_TEST_POLICY = PGPolicy(
        schema=TEST_POLICY.schema,
        signature=TEST_POLICY.signature,
        on_entity=TEST_POLICY.on_entity,
        definition="""
        for update
        to anon_user
        using (true);
        """,
    )

    register_entities([UPDATED_TEST_POLICY], entity_types=[PGPolicy])

    # Autogenerate a new migration
    # It should detect the change we made and produce a "replace_function" statement
    output = run_alembic_command(
        engine=engine,
        command="revision",
        command_kwargs={"autogenerate": True, "rev_id": "2", "message": "replace"},
    )

    migration_replace_path = TEST_VERSIONS_ROOT / "2_replace.py"

    with migration_replace_path.open() as migration_file:
        migration_contents = migration_file.read()

    assert "op.replace_entity" in migration_contents
    assert "op.create_entity" not in migration_contents
    assert "op.drop_entity" not in migration_contents
    assert "from alembic_utils.pg_policy import PGPolicy" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_noop_revision(engine, schema_setup) -> None:
    # Create the view outside of a revision
    with engine.begin() as connection:
        connection.execute(TEST_POLICY.to_sql_statement_create())

    register_entities([TEST_POLICY], entity_types=[PGPolicy])

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
    assert "from alembic_utils.pg_policy import PGPolicy" not in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_drop_revision(engine, schema_setup) -> None:

    # Register no functions locally
    register_entities([], schemas=["public"], entity_types=[PGPolicy])

    # Manually create a SQL function
    with engine.begin() as connection:
        connection.execute(TEST_POLICY.to_sql_statement_create())
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
