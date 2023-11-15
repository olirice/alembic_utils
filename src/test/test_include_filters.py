from alembic_utils.pg_function import PGFunction
from alembic_utils.pg_view import PGView
from alembic_utils.replaceable_entity import register_entities
from alembic_utils.testbase import TEST_VERSIONS_ROOT, run_alembic_command

# The objects marked as "excluded" have names corresponding
# to the filters in src/test/alembic_config/env.py

IncludedView = PGView(
    schema="public",
    signature="A_view",
    definition="select 1 as one",
)

ObjExcludedView = PGView(
    schema="public",
    signature="exclude_obj_view",
    definition="select 1 as one",
)

ReflectedIncludedView = PGView(
    schema="public",
    signature="reflected_view",
    definition="select 1 as one",
)

ReflectedExcludedView = PGView(
    schema="public",
    signature="exclude_name_reflected_view",
    definition="select 1 as one",
)


FuncDef = """
        returns text
        as
        $$ begin return upper(some_text) || 'abc'; end; $$ language PLPGSQL;
        """

IncludedFunc = PGFunction(
    schema="public", signature="toUpper(some_text text default 'my text!')", definition=FuncDef
)

ObjExcludedFunc = PGFunction(
    schema="public",
    signature="exclude_obj_toUpper(some_text text default 'my text!')",
    definition=FuncDef,
)

ReflectedIncludedFunc = PGFunction(
    schema="public",
    signature="reflected_toUpper(some_text text default 'my text!')",
    definition=FuncDef,
)

ReflectedExcludedFunc = PGFunction(
    schema="public",
    signature="exclude_obj_reflected_toUpper(some_text text default 'my text!')",
    definition=FuncDef,
)

reflected_entities = [
    ReflectedIncludedView,
    ReflectedExcludedView,
    ReflectedIncludedFunc,
    ReflectedExcludedFunc,
]

registered_entities = [
    IncludedView,
    ObjExcludedView,
    IncludedFunc,
    ObjExcludedFunc,
]


def test_create_revision_with_filters(engine) -> None:
    with engine.begin() as connection:
        for entity in reflected_entities:
            connection.execute(entity.to_sql_statement_create())
    register_entities(registered_entities)

    run_alembic_command(
        engine=engine,
        command="revision",
        command_kwargs={"autogenerate": True, "rev_id": "1", "message": "filtered_upgrade"},
    )

    migration_create_path = TEST_VERSIONS_ROOT / "1_filtered_upgrade.py"

    with migration_create_path.open() as migration_file:
        migration_contents = migration_file.read()

    assert "op.create_entity(a_view)" in migration_contents
    assert "op.create_entity(toupper)" in migration_contents
    assert "op.drop_entity(reflected_view)" in migration_contents
    assert "op.drop_entity(reflected_toupper)" in migration_contents

    assert not "op.create_entity(exclude_obj_view)" in migration_contents
    assert not "op.create_entity(exclude_obj_toupper)" in migration_contents
    assert not "op.drop_entity(exclude_name_reflected_view)" in migration_contents
    assert not "op.drop_entity(exclude_obj_reflected_toupper)" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})
