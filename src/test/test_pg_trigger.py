import pytest
from sqlalchemy import text

from alembic_utils.exceptions import SQLParseFailure
from alembic_utils.pg_function import PGFunction
from alembic_utils.pg_trigger import PGTrigger
from alembic_utils.replaceable_entity import register_entities
from alembic_utils.testbase import TEST_VERSIONS_ROOT, run_alembic_command


@pytest.fixture(scope="function")
def sql_setup(engine):
    with engine.begin() as connection:
        connection.execute(text(
            """
        create table public.account (
            id serial primary key,
            email text not null
        );
        """
        ))

    yield
    with engine.begin() as connection:
        connection.execute(text("drop table public.account cascade"))


FUNC = PGFunction.from_sql(
    """create function public.downcase_email() returns trigger as $$
begin
    return new;
end;
$$ language plpgsql;
"""
)

TRIG = PGTrigger(
    schema="public",
    signature="lower_account_EMAIL",
    on_entity="public.account",
    definition="""
        BEFORE INSERT ON public.account
        FOR EACH ROW EXECUTE PROCEDURE public.downcase_email()
    """,
)


def test_create_revision(sql_setup, engine) -> None:
    with engine.begin() as connection:
        connection.execute(FUNC.to_sql_statement_create())

    register_entities([FUNC, TRIG], entity_types=[PGTrigger])
    run_alembic_command(
        engine=engine,
        command="revision",
        command_kwargs={"autogenerate": True, "rev_id": "1", "message": "create"},
    )

    migration_create_path = TEST_VERSIONS_ROOT / "1_create.py"

    with migration_create_path.open() as migration_file:
        migration_contents = migration_file.read()

    assert "op.create_entity" in migration_contents
    # Make sure #is_constraint flag was populated
    assert "is_constraint" in migration_contents
    assert "op.drop_entity" in migration_contents
    assert "op.replace_entity" not in migration_contents
    assert "from alembic_utils.pg_trigger import PGTrigger" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_trig_update_revision(sql_setup, engine) -> None:
    with engine.begin() as connection:
        connection.execute(FUNC.to_sql_statement_create())
        connection.execute(TRIG.to_sql_statement_create())

    UPDATED_TRIG = PGTrigger(
        schema=TRIG.schema,
        signature=TRIG.signature,
        on_entity=TRIG.on_entity,
        definition="""
            AFTER INSERT ON public.account
            FOR EACH ROW EXECUTE PROCEDURE public.downcase_email()
        """,
    )

    register_entities([FUNC, UPDATED_TRIG], entity_types=[PGTrigger])

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
    assert "from alembic_utils.pg_trigger import PGTrigger" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})

    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_noop_revision(sql_setup, engine) -> None:
    with engine.begin() as connection:
        connection.execute(FUNC.to_sql_statement_create())
        connection.execute(TRIG.to_sql_statement_create())

    register_entities([FUNC, TRIG], entity_types=[PGTrigger])

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


def test_drop(sql_setup, engine) -> None:
    # Manually create a SQL function
    with engine.begin() as connection:
        connection.execute(FUNC.to_sql_statement_create())
        connection.execute(TRIG.to_sql_statement_create())

    # Register no functions locally
    register_entities([], schemas=["public"], entity_types=[PGTrigger])

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


def test_unparsable() -> None:
    SQL = "create trigger lower_account_email faile fail fail"
    with pytest.raises(SQLParseFailure):
        PGTrigger.from_sql(SQL)


def test_on_entity_schema_not_qualified() -> None:
    SQL = """create trigger lower_account_email
    AFTER INSERT ON account
    FOR EACH ROW EXECUTE PROCEDURE public.downcase_email()
    """
    trigger = PGTrigger.from_sql(SQL)
    assert trigger.schema == "public"


def test_fail_create_sql_statement_create():
    trig = PGTrigger(
        schema=TRIG.schema,
        signature=TRIG.signature,
        on_entity=TRIG.on_entity,
        definition="INVALID DEF",
    )

    with pytest.raises(SQLParseFailure):
        trig.to_sql_statement_create()
