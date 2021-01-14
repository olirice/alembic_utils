import pytest

from alembic_utils.exceptions import SQLParseFailure
from alembic_utils.pg_view import PGView
from alembic_utils.replaceable_entity import register_entities, simulate_entities
from alembic_utils.testbase import TEST_VERSIONS_ROOT, run_alembic_command

VIEW_INDEPENDENT = PGView(
    schema="public",
    signature="independent_view",
    definition="select 1 as one",
)

VIEW_DEPENDENT = PGView(
    schema="public",
    signature="dependent_view",
    definition="select * from public.independent_view",
)



def test_create_revision_with_dependency(engine) -> None:
    register_entities([VIEW_DEPENDENT, VIEW_INDEPENDENT])

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
