import pytest
from sqlalchemy.exc import ProgrammingError

from alembic_utils.exceptions import SQLParseFailure
from alembic_utils.pg_view import PGView
from alembic_utils.replaceable_entity import register_entities
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


def test_create_revision(engine) -> None:
    register_entities([TEST_VIEW], entity_types=[PGView])

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
    with engine.begin() as connection:
        connection.execute(TEST_VIEW.to_sql_statement_create())

    # Update definition of TO_UPPER
    UPDATED_TEST_VIEW = PGView(
        schema=TEST_VIEW.schema, 
        signature=TEST_VIEW.signature, 
        definition="""select *, TRUE as is_updated from pg_views;""",
    )

    register_entities([UPDATED_TEST_VIEW], entity_types=[PGView])

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

    assert "true" in migration_contents.lower()
    assert "false" in migration_contents.lower()
    assert migration_contents.lower().find("true") < migration_contents.lower().find("false")

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_noop_revision(engine) -> None:
    # Create the view outside of a revision
    with engine.begin() as connection:
        connection.execute(TEST_VIEW.to_sql_statement_create())

    register_entities([TEST_VIEW], entity_types=[PGView])

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
    register_entities([], schemas=["DEV"], entity_types=[PGView])

    # Manually create a SQL function
    with engine.begin() as connection:
        connection.execute(TEST_VIEW.to_sql_statement_create())

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
    with engine.begin() as connection:
        connection.execute(TEST_VIEW.to_sql_statement_create())

    # Update definition of TO_UPPER
    # deleted columns from the beginning of the view.
    # this will fail a create or replace statemnt
    # psycopg2.errors.InvalidTableDefinition) cannot drop columns from view
    # and should fail over to drop and then replace (in plpgsql of `create_or_replace_entity` method
    # on pgview

    UPDATED_TEST_VIEW = PGView(
        schema=TEST_VIEW.schema, 
        signature=TEST_VIEW.signature, 
        definition="""select TRUE as is_updated from pg_views"""
    )

    register_entities([UPDATED_TEST_VIEW], entity_types=[PGView])

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


def test_attempt_revision_on_unparsable(engine) -> None:
    BROKEN_VIEW = PGView(schema="public", signature="broken_view", definition="NOPE;")
    register_entities([BROKEN_VIEW], entity_types=[PGView])

    # Reraise of psycopg2.errors.SyntaxError
    with pytest.raises(ProgrammingError):
        run_alembic_command(
            engine=engine,
            command="revision",
            command_kwargs={"autogenerate": True, "rev_id": "1", "message": "create"},
        )


def test_create_revision_with_url_w_colon(engine) -> None:
    """Ensure no regression where views escape colons
    More info at: https://github.com/olirice/alembic_utils/issues/58
    """
    url = "https://something/"
    query = f"SELECT concat('{url}', v::text) FROM generate_series(1,2) x(v)"
    some_view = PGView(schema="public", signature="exa", definition=query)
    register_entities([some_view], entity_types=[PGView])

    output = run_alembic_command(
        engine=engine,
        command="revision",
        command_kwargs={"autogenerate": True, "rev_id": "1", "message": "create"},
    )

    migration_create_path = TEST_VERSIONS_ROOT / "1_create.py"

    with migration_create_path.open() as migration_file:
        migration_contents = migration_file.read()

    assert url in migration_contents
    assert "op.create_entity" in migration_contents
    assert "op.drop_entity" in migration_contents
    assert "op.replace_entity" not in migration_contents
    assert "from alembic_utils.pg_view import PGView" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_view_contains_colon(engine) -> None:
    TEST_SEMI_VIEW = PGView(
        schema="public",
        signature="sample",
        definition="select ':' as myfield, '1'::int as othi",
    )
    # NOTE: if a definition contains something that looks like a bind parameter e.g. :a
    # an exception is raised. This test confirms that non-bind-parameter usage of colon
    # is a non-issue

    register_entities([TEST_SEMI_VIEW], entity_types=[PGView])

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
