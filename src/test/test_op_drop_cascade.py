import pytest
from sqlalchemy.exc import InternalError

from alembic_utils.pg_view import PGView
from alembic_utils.replaceable_entity import register_entities
from alembic_utils.testbase import TEST_VERSIONS_ROOT, run_alembic_command

A = PGView(
    schema="public",
    signature="A_view",
    definition="select 1::integer as one",
)

B_A = PGView(
    schema="public",
    signature="B_view",
    definition='select * from public."A_view"',
)


def test_drop_fails_without_cascade(engine) -> None:

    with engine.begin() as connection:
        connection.execute(A.to_sql_statement_create())
        connection.execute(B_A.to_sql_statement_create())

    register_entities([B_A], schemas=["DEV"], entity_types=[PGView])

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

    with pytest.raises(InternalError):
        # sqlalchemy.exc.InternalError: (psycopg2.errors.DependentObjectsStillExist) cannot drop view "A_view" because other objects depend on it
        run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})


def test_drop_fails_with_cascade(engine, sess) -> None:

    with engine.begin() as connection:
        connection.execute(A.to_sql_statement_create())
        connection.execute(B_A.to_sql_statement_create())

    register_entities([B_A], schemas=["DEV"], entity_types=[PGView])

    output = run_alembic_command(
        engine=engine,
        command="revision",
        command_kwargs={"autogenerate": True, "rev_id": "1", "message": "drop"},
    )

    migration_create_path = TEST_VERSIONS_ROOT / "1_drop.py"

    with migration_create_path.open() as migration_file:
        migration_contents = migration_file.read()

    assert "op.drop_entity" in migration_contents
    assert "op.drop_entity(a_view)" in migration_contents

    migration_contents = migration_contents.replace(
        "op.drop_entity(a_view)", "op.drop_entity(a_view, cascade=True)"
    )

    with migration_create_path.open("w") as migration_file:
        migration_file.write(migration_contents)

    assert "op.create_entity" in migration_contents
    assert "from alembic_utils" in migration_contents
    assert migration_contents.index("op.drop_entity") < migration_contents.index("op.create_entity")

    # Cascade drops *B_A* and succeeds
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})

    # Make sure the drop ocurred
    all_views = PGView.from_database(sess, "public")
    assert len(all_views) == 0
