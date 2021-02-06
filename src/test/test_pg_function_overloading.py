from alembic_utils.pg_function import PGFunction
from alembic_utils.replaceable_entity import register_entities
from alembic_utils.testbase import TEST_VERSIONS_ROOT, run_alembic_command

TO_FLOAT_FROM_INT = PGFunction(
    schema="public",
    signature="to_float(x integer)",
    definition="""
        returns float
        as
        $$ select x::float $$ language SQL;
        """,
)

TO_FLOAT_FROM_TEXT = PGFunction(
    schema="public",
    signature="to_float(x text)",
    definition="""
        returns float
        as
        $$ select x::float $$ language SQL;
        """,
)


def test_create_revision(engine) -> None:
    register_entities([TO_FLOAT_FROM_INT, TO_FLOAT_FROM_TEXT], entity_types=[PGFunction])

    run_alembic_command(
        engine=engine,
        command="revision",
        command_kwargs={"autogenerate": True, "rev_id": "1", "message": "create"},
    )

    migration_create_path = TEST_VERSIONS_ROOT / "1_create.py"

    with migration_create_path.open() as migration_file:
        migration_contents = migration_file.read()

    assert migration_contents.count("op.create_entity") == 2
    assert "op.drop_entity" in migration_contents
    assert "op.replace_entity" not in migration_contents
    assert "from alembic_utils.pg_function import PGFunction" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_update_revision(engine) -> None:
    engine.execute(TO_FLOAT_FROM_INT.to_sql_statement_create())
    engine.execute(TO_FLOAT_FROM_TEXT.to_sql_statement_create())

    UPDATE = PGFunction(
        schema="public",
        signature="to_float(x integer)",
        definition="""
            returns float
            as
            $$ select x::text::float $$ language SQL;
            """,
    )

    register_entities([UPDATE, TO_FLOAT_FROM_TEXT], entity_types=[PGFunction])

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

    # One up and one down
    assert migration_contents.count("op.replace_entity") == 2
    assert "op.create_entity" not in migration_contents
    assert "op.drop_entity" not in migration_contents
    assert "from alembic_utils.pg_function import PGFunction" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})

    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})
