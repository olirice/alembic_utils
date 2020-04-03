from alembic_utils import TEST_VERSIONS_ROOT
from alembic_utils.pg_function import PGFunction, register_functions
from alembic_utils.testbase import run_alembic_command

TO_UPPER = PGFunction(
    schema="public",
    signature="to_upper(some_text text)",
    definition="""
        returns text
        as
        $$ select upper(some_text) || 'abc' $$ language SQL;
        """,
)


def test_migration_create_function(engine, reset: None) -> None:
    register_functions([TO_UPPER])
    output = run_alembic_command(
        engine=engine,
        command="revision",
        command_kwargs={"autogenerate": True, "rev_id": "1", "message": "create"},
    )

    migration_create_path = TEST_VERSIONS_ROOT / "1_create.py"

    with migration_create_path.open() as migration_file:
        migration_contents = migration_file.read()

    assert "op.create_function" in migration_contents
    assert "op.drop_function" in migration_contents
    assert "from alembic_utils import PGFunction" in migration_contents


def test_migration_replace_function(engine, reset: None) -> None:
    register_functions([TO_UPPER])

    output = run_alembic_command(
        engine=engine,
        command="revision",
        command_kwargs={"autogenerate": True, "rev_id": "1", "message": "create"},
    )

    migration_create_path = TEST_VERSIONS_ROOT / "1_create.py"

    with migration_create_path.open() as migration_file:
        migration_contents = migration_file.read()

    assert "op.create_function" in migration_contents
    assert "op.drop_function" in migration_contents
    assert "from alembic_utils import PGFunction" in migration_contents

    # Apply the first imgration
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})

    # Update definition of TO_UPPER
    TO_UPPER.definition = """
        returns text
        as
        $$ select upper(some_text) || 'def' $$ language SQL;
    """

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

    assert "op.replace_function" in migration_contents
    assert "from alembic_utils import PGFunction" in migration_contents

    # Create a third migration without making changes.
    # This should result in no create, drop or replace statements
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})

    output = run_alembic_command(
        engine=engine,
        command="revision",
        command_kwargs={"autogenerate": True, "rev_id": "3", "message": "do_nothing"},
    )
    migration_replace_path = TEST_VERSIONS_ROOT / "3_do_nothing.py"

    with migration_replace_path.open() as migration_file:
        migration_contents = migration_file.read()

    assert "op.create_function" not in migration_contents
    assert "op.drop_function" not in migration_contents
    assert "op.replace_function" not in migration_contents
    assert "from alembic_utils import PGFunction" not in migration_contents

    # Execute the downgrades
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})
