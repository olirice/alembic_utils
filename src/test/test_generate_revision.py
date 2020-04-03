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
