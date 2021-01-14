import pytest

from alembic_utils.exceptions import SQLParseFailure
from alembic_utils.pg_view import PGView
from alembic_utils.replaceable_entity import register_entities, simulate_entities
from alembic_utils.testbase import TEST_VERSIONS_ROOT, run_alembic_command

TEST_VIEW = PGView(
    schema="DEV", signature="testExample", definition="select *, FALSE as is_updated from pg_views"
)


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

    SQL = "create view public.some_view(one) as select 1 one;"
    try:
        view = PGView.from_sql(SQL)
        assert view.signature == "some_view"
    except SQLParseFailure:
        pytest.fail(f"Unexpected SQLParseFailure for view {SQL}")


def test_find_no_match(engine) -> None:
    with engine.connect() as connection:
        maybe_found = TEST_VIEW.get_database_definition(connection)

    assert maybe_found is None


def test_teardown_temp_schema_on_error(engine) -> None:
    """Make sure the temporary schema gets town down when the simulated entity fails"""
    SQL = "create or replace view public.some_view as INVALID SQL!;"
    view = PGView.from_sql(SQL)

    with engine.connect() as connection:
        with pytest.raises(Exception):
            with simulate_entities(connection, [view]):
                pass

        maybe_schema = connection.execute(
            "select * from pg_namespace where nspname = 'alembic_utils';"
        ).fetchone()
        assert maybe_schema is None


def test_create_revision(engine) -> None:
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

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_update_revision(engine) -> None:
    # Create the view outside of a revision
    engine.execute(TEST_VIEW.to_sql_statement_create())

    # Update definition of TO_UPPER
    UPDATED_TEST_VIEW = PGView(
        TEST_VIEW.schema, TEST_VIEW.signature, """select *, TRUE as is_updated from pg_views;"""
    )

    register_entities([UPDATED_TEST_VIEW])

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

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_noop_revision(engine) -> None:
    # Create the view outside of a revision
    engine.execute(TEST_VIEW.to_sql_statement_create())

    register_entities([TEST_VIEW])

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
    register_entities([], schemas=["DEV"])

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

    # import pdb; pdb.set_trace()

    assert "op.drop_entity" in migration_contents
    assert "op.create_entity" in migration_contents
    assert "from alembic_utils" in migration_contents
    assert migration_contents.index("op.drop_entity") < migration_contents.index("op.create_entity")

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_update_create_or_replace_failover_to_drop_add(engine) -> None:
    # Create the view outside of a revision
    engine.execute(TEST_VIEW.to_sql_statement_create())

    # Update definition of TO_UPPER
    # deleted columns from the beginning of the view.
    # this will fail a create or replace statemnt
    # psycopg2.errors.InvalidTableDefinition) cannot drop columns from view
    # and should fail over to drop and then replace (in plpgsql of `create_or_replace_entity` method
    # on pgview

    UPDATED_TEST_VIEW = PGView(
        TEST_VIEW.schema, TEST_VIEW.signature, """select TRUE as is_updated from pg_views"""
    )

    register_entities([UPDATED_TEST_VIEW])

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

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})
