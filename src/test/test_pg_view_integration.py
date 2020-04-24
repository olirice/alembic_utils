from alembic_utils.pg_view import PGView
from alembic_utils.replaceable_entity import register_entities
from alembic_utils.testbase import TEST_VERSIONS_ROOT, run_alembic_command

TEST_VIEW = PGView(
    schema="public",
    signature="test_example",
    definition="select *, FALSE as is_updated from pg_views",
)


def test_create_and_revise(engine, reset) -> None:
    register_entities([TEST_VIEW])

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
    assert "from alembic_utils.pg_view import PGView" in migration_contents

    # Apply the first imgration
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})

    # Update definition of TO_UPPER
    TEST_VIEW.definition = """select *, TRUE as is_updated from pg_views"""

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
    assert "from alembic_utils.pg_view import PGView" in migration_contents

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

    # Execute the downgrades
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_drop(engine, reset: None) -> None:

    # Register no functions locally
    register_entities([], schemas=["public"])

    # Manually create a SQL function
    engine.execute(TEST_VIEW.to_sql_statement_create())

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

    # Apply the first imgration
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})

    # Make sure function no longer exists
    with engine.connect() as connection:
        assert TEST_VIEW.get_database_definition(connection) is None
