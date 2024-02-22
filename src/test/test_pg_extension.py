import pytest

from alembic_utils.pg_extension import PGExtension
from alembic_utils.replaceable_entity import register_entities
from alembic_utils.testbase import TEST_VERSIONS_ROOT, run_alembic_command

TEST_EXT = PGExtension(schema="public", signature="uuid-ossp")


def test_create_revision(engine) -> None:
    register_entities([TEST_EXT], entity_types=[PGExtension])

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
    assert "from alembic_utils.pg_extension import PGExtension" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_create_or_replace_raises():
    with pytest.raises(NotImplementedError):
        TEST_EXT.to_sql_statement_create_or_replace()


def test_update_is_unreachable(engine) -> None:
    # Updates are not possible. The only parameter that may change is
    # schema, and that will result in a drop and create due to schema
    # scoping assumptions made for all other entities

    # Create the view outside of a revision
    with engine.begin() as connection:
        connection.execute(TEST_EXT.to_sql_statement_create())

    UPDATED_TEST_EXT = PGExtension(
        schema="DEV", 
        signature=TEST_EXT.signature
    )

    register_entities([UPDATED_TEST_EXT], schemas=["public", "DEV"], entity_types=[PGExtension])

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

    assert "op.replace_entity" not in migration_contents
    assert "from alembic_utils.pg_extension import PGExtension" in migration_contents


def test_noop_revision(engine) -> None:
    # Create the view outside of a revision
    with engine.begin() as connection:
        connection.execute(TEST_EXT.to_sql_statement_create())

    register_entities([TEST_EXT], entity_types=[PGExtension])

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


def test_drop_revision(engine) -> None:
    # Register no functions locally
    register_entities([], entity_types=[PGExtension])

    # Manually create a SQL function
    with engine.begin() as connection:
        connection.execute(TEST_EXT.to_sql_statement_create())

    output = run_alembic_command(
        engine=engine,
        command="revision",
        command_kwargs={"autogenerate": True, "rev_id": "1", "message": "drop"},
    )

    migration_create_path = TEST_VERSIONS_ROOT / "1_drop.py"

    with migration_create_path.open() as migration_file:
        migration_contents = migration_file.read()

    assert "op.drop_entity" in migration_contents
    assert "op.create_entity" in migration_contents
    assert "from alembic_utils" in migration_contents
    assert migration_contents.index("op.drop_entity") < migration_contents.index("op.create_entity")

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})
