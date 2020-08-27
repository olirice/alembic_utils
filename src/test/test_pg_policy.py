from alembic_utils.pg_policy import PGPolicy
from alembic_utils.replaceable_entity import register_entities
from alembic_utils.testbase import TEST_VERSIONS_ROOT, run_alembic_command
import functools

# The role alem_user must be defined

TEST_POLICY = PGPolicy(
    schema="DEV",
    signature="postgres_all.user",
    definition="""
        AS permissive
        FOR ALL
        TO alem_user
        USING ("id" = 1)
        WITH CHECK ("id" = 1);
        """,
)


def with_table(func):
    @functools.wraps(func)
    def wrapper(engine) -> None:
        # A postgres policy is applied to a table
        engine.execute('CREATE TABLE "DEV".user (id integer)')

        func(engine)

    return wrapper


@with_table
def test_create_revision(engine) -> None:
    register_entities([TEST_POLICY])

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
    assert "from alembic_utils.pg_policy import PGPolicy" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


@with_table
def test_update_revision(engine) -> None:
    engine.execute(TEST_POLICY.to_sql_statement_create())

    # Update definition of TEST_POLICY
    UPDATED_TEST_POLICY = PGPolicy(
        TEST_POLICY.schema,
        TEST_POLICY.signature,
        r"""AS restrictive
        FOR UPDATE
        TO alem_user
        USING ("id" = 2)
        WITH CHECK ("id" = 2);""",
    )

    register_entities([UPDATED_TEST_POLICY])

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
    assert "from alembic_utils.pg_policy import PGPolicy" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})

    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


@with_table
def test_noop_revision(engine) -> None:
    engine.execute(TEST_POLICY.to_sql_statement_create())

    register_entities([TEST_POLICY])

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


@with_table
def test_drop_revision(engine) -> None:

    # Register no functions locally
    register_entities([], schemas=["DEV"])

    # Manually create a SQL function
    engine.execute(TEST_POLICY.to_sql_statement_create())

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