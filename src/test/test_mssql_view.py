import os
import shutil
import time

import docker
import pytest
from parse import parse
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError

from alembic_utils.exceptions import SQLParseFailure
from alembic_utils.mssql_view import MSSQLView
from alembic_utils.replaceable_entity import register_entities
from alembic_utils.testbase import (
    TEST_VERSIONS_ROOT,
    reset_event_listener_registry,
    run_alembic_command,
)

PYTEST_DB = (
    "mssql+pyodbc://sa:PassW0rd1234@localhost:1433/alem_db?driver=ODBC+Driver+17+for+SQL+Server"
)


@pytest.fixture(scope="session")
def maybe_start_mssql():
    """Creates a mssql docker container that can be connected
    to using the PYTEST_DB connection string"""

    container_name = "alembic_utils_mssql"
    image = "mcr.microsoft.com/mssql/server:2017-latest"

    connection_template = (
        "mssql+pyodbc://{user}:{pw}@{host}:{port:d}/{db}?driver=ODBC+Driver+17+for+SQL+Server"
    )
    conn_args = parse(connection_template, PYTEST_DB)

    # Don't attempt to instantiate a container if
    # we're on CI
    if "GITHUB_SHA" in os.environ:
        yield
        return

    client = docker.from_env()
    try:
        mssql_container = client.containers.get(container_name)
        is_running = mssql_container.status == "running"
    except docker.errors.NotFound:
        is_running = False

    if is_running:
        yield
        return

    client.containers.run(
        image=image,
        name=container_name,
        ports={conn_args["port"]: conn_args["port"]},
        environment={
            "ACCEPT_EULA": "Y",
            "SA_PASSWORD": conn_args["pw"],
        },
        detach=True,
        # auto_remove=True,
        command=[
            "/bin/bash",
            "-c",
            f"( /opt/mssql/bin/sqlservr & ) | grep -q 'Service Broker manager has started' && /opt/mssql-tools/bin/sqlcmd -S {conn_args['host']} -U sa -P {conn_args['pw']} -Q 'CREATE DATABASE {conn_args['db']}' && sleep infinity",
        ],
    )

    # Give sql server 10 seconds to get ready to serve connections, or it might fail
    time.sleep(10)

    yield
    # subprocess.call(["docker", "stop", container_name])
    return


@pytest.fixture(scope="session")
def raw_engine(maybe_start_mssql: None):
    """sqlalchemy engine fixture"""
    eng = create_engine(PYTEST_DB, connect_args={"connect_timeout": 5})
    yield eng
    eng.dispose()


@pytest.fixture(scope="function")
def engine(raw_engine):
    """Engine that has been reset between tests"""

    def run_cleaners():
        reset_event_listener_registry()

        raw_engine.execute("DROP VIEW IF EXISTS [public_mssql].[some_view]")
        raw_engine.execute("DROP SCHEMA IF EXISTS [public_mssql]")
        raw_engine.execute("CREATE SCHEMA [public_mssql]")

        raw_engine.execute("DROP VIEW IF EXISTS [DEV].[testExample]")
        raw_engine.execute("DROP SCHEMA IF EXISTS [DEV]")
        raw_engine.execute("CREATE SCHEMA [DEV]")

        # Remove any migrations that were left behind
        TEST_VERSIONS_ROOT.mkdir(exist_ok=True, parents=True)
        shutil.rmtree(TEST_VERSIONS_ROOT)
        TEST_VERSIONS_ROOT.mkdir(exist_ok=True, parents=True)

    run_cleaners()

    yield raw_engine

    run_cleaners()


TEST_VIEW = MSSQLView(
    schema="DEV",
    signature="testExample",
    definition="select *, 0 as is_updated from INFORMATION_SCHEMA.views",
)


def test_unparsable_view() -> None:
    SQL = "create or alter vew public.some_view as select 1 one;"
    with pytest.raises(SQLParseFailure):
        view = MSSQLView.from_sql(SQL)


def test_parsable_body() -> None:
    SQL = "create or alter view public.some_view as select 1 one;"
    try:
        view = MSSQLView.from_sql(SQL)
    except SQLParseFailure:
        pytest.fail(f"Unexpected SQLParseFailure for view {SQL}")

    SQL = "create view public.some_view(one) as select 1 one;"
    try:
        view = MSSQLView.from_sql(SQL)
        assert view.signature == "some_view"
    except SQLParseFailure:
        pytest.fail(f"Unexpected SQLParseFailure for view {SQL}")


def test_parsable_body_with_square_brackets() -> None:
    SQL = "create or alter view [public].[some view] as select 1 one;"
    try:
        view = MSSQLView.from_sql(SQL)
    except SQLParseFailure:
        pytest.fail(f"Unexpected SQLParseFailure for view {SQL}")

    SQL = "create view [public].[some view](one) as select 1 one;"
    try:
        view = MSSQLView.from_sql(SQL)
        assert view.schema == "public"
        assert view.signature == "some view"
    except SQLParseFailure:
        pytest.fail(f"Unexpected SQLParseFailure for view {SQL}")


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
    assert "from alembic_utils.mssql_view import MSSQLView" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_update_revision(engine) -> None:
    # Create the view outside of a revision
    engine.execute(TEST_VIEW.to_sql_statement_create())

    # Update definition of TO_UPPER
    UPDATED_TEST_VIEW = MSSQLView(
        TEST_VIEW.schema,
        TEST_VIEW.signature,
        """select *, 1 as is_updated from INFORMATION_SCHEMA.views;""",
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
    assert "from alembic_utils.mssql_view import MSSQLView" in migration_contents

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

    UPDATED_TEST_VIEW = MSSQLView(
        TEST_VIEW.schema,
        TEST_VIEW.signature,
        """select 1 as is_updated from INFORMATION_SCHEMA.views""",
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
    assert "from alembic_utils.mssql_view import MSSQLView" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_attempt_revision_on_unparsable(engine) -> None:
    BROKEN_VIEW = MSSQLView(schema="public", signature="broken_view", definition="NOPE;")
    register_entities([BROKEN_VIEW])

    # Reraise of pyodbc.ProgrammingError
    with pytest.raises(ProgrammingError):
        run_alembic_command(
            engine=engine,
            command="revision",
            command_kwargs={"autogenerate": True, "rev_id": "1", "message": "create"},
        )
