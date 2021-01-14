import pytest

from alembic_utils.exceptions import SQLParseFailure
from alembic_utils.pg_materialized_view import PGMaterializedView
from alembic_utils.replaceable_entity import register_entities, simulate_entities
from alembic_utils.testbase import TEST_VERSIONS_ROOT, run_alembic_command

TEST_MAT_VIEW = PGMaterializedView(
    schema="DEV",
    signature="test_mat_view",
    definition="select *, FALSE as is_updated from pg_matviews",
    with_data=True,
)


def test_unparsable_view() -> None:
    SQL = "create or replace materialized vew public.some_view as select 1 one;"
    with pytest.raises(SQLParseFailure):
        view = PGMaterializedView.from_sql(SQL)


def test_parsable_body() -> None:
    SQL = "create materialized view public.some_view as select 1 one;"
    try:
        view = PGMaterializedView.from_sql(SQL)
    except SQLParseFailure:
        pytest.fail(f"Unexpected SQLParseFailure for view {SQL}")

    SQL = "create materialized view public.some_view as select 1 one with data;"
    try:
        view = PGMaterializedView.from_sql(SQL)
        assert view.with_data
    except SQLParseFailure:
        pytest.fail(f"Unexpected SQLParseFailure for view {SQL}")

    SQL = "create materialized view public.some_view as select 1 one with no data;"
    try:
        view = PGMaterializedView.from_sql(SQL)
        assert not view.with_data
    except SQLParseFailure:
        pytest.fail(f"Unexpected SQLParseFailure for view {SQL}")

    SQL = "create materialized view public.some_view(one) as select 1 one;"
    try:
        view = PGMaterializedView.from_sql(SQL)
        assert view.signature == "some_view"
    except SQLParseFailure:
        pytest.fail(f"Unexpected SQLParseFailure for view {SQL}")


def test_find_no_match(sess) -> None:
    maybe_found = TEST_MAT_VIEW.get_database_definition(sess)
    assert maybe_found is None


def test_create_revision(engine) -> None:
    register_entities([TEST_MAT_VIEW])

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
    assert "from alembic_utils.pg_materialized_view import PGMaterializedView" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_update_revision(engine) -> None:
    # Create the view outside of a revision
    engine.execute(TEST_MAT_VIEW.to_sql_statement_create())

    # Update definition of TO_UPPER
    UPDATED_TEST_MAT_VIEW = PGMaterializedView(
        TEST_MAT_VIEW.schema,
        TEST_MAT_VIEW.signature,
        """select *, TRUE as is_updated from pg_matviews""",
        with_data=TEST_MAT_VIEW.with_data,
    )

    register_entities([UPDATED_TEST_MAT_VIEW])

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
    assert "from alembic_utils.pg_materialized_view import PGMaterializedView" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_noop_revision(engine) -> None:
    # Create the view outside of a revision
    engine.execute(TEST_MAT_VIEW.to_sql_statement_create())

    register_entities([TEST_MAT_VIEW])

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
    engine.execute(TEST_MAT_VIEW.to_sql_statement_create())

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
