from typing import List

from sqlalchemy import text

from alembic_utils.pg_procedure import PGProcedure
from alembic_utils.replaceable_entity import register_entities
from alembic_utils.testbase import TEST_VERSIONS_ROOT, run_alembic_command

TO_UPPER = PGProcedure(
    schema="public",
    signature="toUpper (some_text text default 'my text!')",
    definition="""
        language PLPGSQL as $$
        declare result text;
        begin result = upper(some_text) || 'abc'; end; $$;
        """,
)


def test_trailing_whitespace_stripped():
    sql_statements: List[str] = [
        str(TO_UPPER.to_sql_statement_create()),
        str(next(iter(TO_UPPER.to_sql_statement_create_or_replace()))),
        str(TO_UPPER.to_sql_statement_drop()),
    ]

    for statement in sql_statements:
        print(statement)
        assert '"toUpper"' in statement
        assert not '"toUpper "' in statement


def test_create_revision(engine) -> None:
    register_entities([TO_UPPER], entity_types=[PGProcedure])

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
    assert "from alembic_utils.pg_procedure import PGProcedure" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_update_revision(engine) -> None:
    with engine.begin() as connection:
        connection.execute(TO_UPPER.to_sql_statement_create())

    # Update definition of TO_UPPER
    UPDATED_TO_UPPER = PGProcedure(
        TO_UPPER.schema,
        TO_UPPER.signature,
        r'''
    language SQL as $$
    select upper(some_text) || 'def'  -- """ \n \\
    $$''',
    )

    register_entities([UPDATED_TO_UPPER], entity_types=[PGProcedure])

    # Autogenerate a new migration
    # It should detect the change we made and produce a "replace_procedure" statement
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
    assert "from alembic_utils.pg_procedure import PGProcedure" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})

    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_noop_revision(engine) -> None:
    with engine.begin() as connection:
        connection.execute(TO_UPPER.to_sql_statement_create())

    register_entities([TO_UPPER], entity_types=[PGProcedure])

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


def test_drop(engine) -> None:
    # Manually create a SQL procedure
    with engine.begin() as connection:
        connection.execute(TO_UPPER.to_sql_statement_create())

    # Register no procedure locally
    register_entities([], schemas=["public"], entity_types=[PGProcedure])

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
    # Error was occuring in drop statement when procedure had no parameters
    # related to parameter parsing to drop default statements

    SIDE_EFFECT = PGProcedure(
        schema="public",
        signature="side_effect()",
        definition="""
            language SQL as $$
            select 1; $$;
            """,
    )

    # Register no procedures locally
    register_entities([SIDE_EFFECT], schemas=["public"], entity_types=[PGProcedure])

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


def test_ignores_extension_procedures(engine) -> None:
    # Extensions contain procedures and don't have local representations
    # Unless they are excluded, every autogenerate migration will produce
    # drop statements for those procedures
    try:
        with engine.begin() as connection:
            connection.execute(text("create extension if not exists unaccent;"))
        register_entities([], schemas=["public"], entity_types=[PGProcedure])
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


def test_plpgsql_colon_esacpe(engine) -> None:
    # PGProcedure.__init__ overrides colon escapes for plpgsql
    # because := should not be escaped for sqlalchemy.text
    # if := is escaped, an exception would be raised

    PLPGSQL_FUNC = PGProcedure(
        schema="public",
        signature="some_proc(some_text text)",
        definition="""
            language plpgsql as $$
            declare
                copy_o_text text;
            begin
                copy_o_text := some_text;
            end;
            $$;
            """,
    )

    register_entities([PLPGSQL_FUNC], entity_types=[PGProcedure])

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
    assert "from alembic_utils.pg_procedure import PGProcedure" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})
