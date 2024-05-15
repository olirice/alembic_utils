from dataclasses import dataclass
import logging

import pytest
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

TO_UPPER_UPDATED_DEF = r'''returns text as
    $$
    select upper(some_text) || 'def'  -- """ \n \\
    $$ language SQL immutable strict;'''


CLEAR_CACHE = PGFunction(
    schema="public",
    signature="clear_cache(obj_id uuid)",
    definition="""
LANGUAGE plpgsql
AS $procedure$
DECLARE
BEGIN
    CALL clear_parent_obj(obj_id, true);
END;
$procedure$
""",
    is_proc=True,
)

CLEAR_CACHE_UPDATED_DEF = """
AS $procedure$
DECLARE
BEGIN
    CALL clear_parent_obj(obj_id, true);
    CALL clear_child_obj(obj_id, true);
END;
$procedure$ language plpgsql
"""


@dataclass
class FuncMigrationTestCase:
    name: str
    func: PGFunction
    updated_def: str


_TEST_CASES = [
    FuncMigrationTestCase(
        name="toUpper",
        func=TO_UPPER,
        updated_def=TO_UPPER_UPDATED_DEF,
    ),
    FuncMigrationTestCase(
        name="clear_cache",
        func=CLEAR_CACHE,
        updated_def=CLEAR_CACHE_UPDATED_DEF,
    ),
]


logger = logging.getLogger(__name__)


@pytest.mark.parametrize("data", _TEST_CASES, ids=[tc.name for tc in _TEST_CASES])
def test_create_revision(engine, data) -> None:
    register_entities([data.func], entity_types=[PGFunction])

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


@pytest.mark.parametrize("data", _TEST_CASES, ids=[tc.name for tc in _TEST_CASES])
def test_update_revision(engine, data) -> None:
    with engine.begin() as connection:
        connection.execute(data.func.to_sql_statement_create())

    # Update definition
    updated_func = PGFunction(
        schema= data.func.schema,
        signature=data.func.signature,
        definition=data.updated_def,
        is_proc=data.func.is_proc,
    )

    register_entities([updated_func], entity_types=[PGFunction])

    # Autogenerate a new migration
    # It should detect the change we made and produce a "replace_function" statement
    run_alembic_command(
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
    assert "from alembic_utils.pg_function import PGFunction" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})

    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


@pytest.mark.parametrize("data", _TEST_CASES, ids=[tc.name for tc in _TEST_CASES])
def test_noop_revision(engine, data) -> None:
    with engine.begin() as connection:
        connection.execute(data.func.to_sql_statement_create())

    register_entities([data.func], entity_types=[PGFunction])

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


@pytest.mark.parametrize("data", _TEST_CASES, ids=[tc.name for tc in _TEST_CASES])
def test_drop(engine, data) -> None:
    # Manually create a SQL function
    with engine.begin() as connection:
        connection.execute(data.func.to_sql_statement_create())

    # Register no functions locally
    register_entities([], schemas=["public"], entity_types=[PGFunction])

    run_alembic_command(
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


def test_has_no_parameters(engine) -> None:
    # Error was occuring in drop statement when function had no parameters
    # related to parameter parsing to drop default statements

    SIDE_EFFECT = PGFunction(
        schema="public",
        signature="side_effect()",
        definition="""
            returns integer
            as
            $$ select 1; $$ language SQL;
            """,
    )

    # Register no functions locally
    register_entities([SIDE_EFFECT], schemas=["public"], entity_types=[PGFunction])

    run_alembic_command(
        engine=engine,
        command="revision",
        command_kwargs={"autogenerate": True, "rev_id": "1", "message": "no_arguments"},
    )

    migration_create_path = TEST_VERSIONS_ROOT / "1_no_arguments.py"

    with migration_create_path.open() as migration_file:
        migration_contents = migration_file.read()

    assert "op.drop_entity" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_ignores_extension_functions(engine) -> None:
    # Extensions contain functions and don't have local representations
    # Unless they are excluded, every autogenerate migration will produce
    # drop statements for those functions
    try:
        with engine.begin() as connection:
            connection.execute(text("create extension if not exists unaccent;"))
        register_entities([], schemas=["public"], entity_types=[PGFunction])
        run_alembic_command(
            engine=engine,
            command="revision",
            command_kwargs={"autogenerate": True, "rev_id": "1", "message": "no_drops"},
        )

        migration_create_path = TEST_VERSIONS_ROOT / "1_no_drops.py"

        with migration_create_path.open() as migration_file:
            migration_contents = migration_file.read()

        assert "op.drop_entity" not in migration_contents
    finally:
        with engine.begin() as connection:
            connection.execute(text("drop extension if exists unaccent;"))


def test_plpgsql_colon_escape(engine) -> None:
    # PGFunction.__init__ overrides colon escapes for plpgsql
    # because := should not be escaped for sqlalchemy.text
    # if := is escaped, an exception would be raised

    PLPGSQL_FUNC = PGFunction(
        schema="public",
        signature="some_func(some_text text)",
        definition="""
            returns text
            as
            $$
            declare
                copy_o_text text;
            begin
                copy_o_text := some_text;
                return copy_o_text;
            end;
            $$ language plpgsql
            """,
    )

    register_entities([PLPGSQL_FUNC], entity_types=[PGFunction])

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
