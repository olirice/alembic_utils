import pytest

from alembic_utils.exceptions import FailedToGenerateComparable, SQLParseFailure
from alembic_utils.pg_function import PGFunction
from alembic_utils.pg_trigger import PGTrigger
from alembic_utils.replaceable_entity import register_entities
from alembic_utils.testbase import TEST_VERSIONS_ROOT, run_alembic_command


@pytest.fixture(scope="function")
def sql_setup(engine):
    conn = engine
    conn.execute(
        """
    create table public.account (
        id serial primary key,
        email text not null
    );


    """
    )

    yield
    conn.execute("drop table public.account cascade")


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
    signature="lower_account_email",
    definition="""
        BEFORE INSERT ON public.account
        FOR EACH ROW EXECUTE FUNCTION public.downcase_email()
    """,
)


def test_create_revision(sql_setup, engine) -> None:
    register_entities([FUNC, TRIG])
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
    assert "from alembic_utils.pg_trigger import PGTrigger" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})


def test_trig_update_revision(sql_setup, engine) -> None:
    engine.execute(FUNC.to_sql_statement_create())
    engine.execute(TRIG.to_sql_statement_create())

    UPDATED_TRIG = PGTrigger(
        schema="public",
        signature="lower_account_email",
        definition="""
            AFTER INSERT ON public.account
            FOR EACH ROW EXECUTE FUNCTION public.downcase_email()
        """,
    )

    register_entities([FUNC, UPDATED_TRIG])

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
    engine.execute(FUNC.to_sql_statement_create())
    engine.execute(TRIG.to_sql_statement_create())

    register_entities([FUNC, TRIG])

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
    engine.execute(FUNC.to_sql_statement_create())
    engine.execute(TRIG.to_sql_statement_create())

    # Register no functions locally
    register_entities([], schemas=["public"])

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
    FOR EACH ROW EXECUTE FUNCTION public.downcase_email()
    """
    with pytest.raises(SQLParseFailure):
        PGTrigger.from_sql(SQL)


def test_fail_create_sql_statement_create():
    trig = PGTrigger(schema="public", signature="lower_account_email", definition="INVALID DEF")

    with pytest.raises(SQLParseFailure):
        trig.to_sql_statement_create()


def test_get_definition_comparable_does_not_exist_yet(sql_setup, engine):
    engine.execute(FUNC.to_sql_statement_create())
    # for coverage
    assert TRIG.get_definition_comparable(engine) is not None


def test_get_definition_comparable_invalid_sql(sql_setup, engine):
    trig = PGTrigger(schema="public", signature="lower_account_email", definition="INVALID DEF")
    with pytest.raises(FailedToGenerateComparable):
        assert TRIG.get_definition_comparable(engine) is not None
