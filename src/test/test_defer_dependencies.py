from alembic_utils.pg_materialized_view import PGMaterializedView
from alembic_utils.replaceable_entity import register_entities
from alembic_utils.testbase import run_alembic_command

TEST_MAT_VIEW = PGMaterializedView(
    schema="DEV",
    signature="test_mat_view",
    definition="select *, FALSE as is_updated from pg_matviews",
    with_data=True,
)


def test_works_when_dependencies_exist(engine) -> None:
    engine.execute(TEST_MAT_VIEW.to_sql_statement_create())

    # Make a dependency on the TEST_MAT_VIEW
    # Materialized views don't have a create or replace statement
    # So replace_entity is defined as a drop and then a create
    # the simulator will fail to simulate the drop and complain about dependent entities
    # if they exist

    # The dependency resolver defers dependent entities while we figure out what
    # migration op is needed and then rolls them all back in a transaction

    engine.execute('create view public.abc as select * from "DEV".test_mat_view;')
    engine.execute('create view "DEV".def as select * from "DEV".test_mat_view;')

    register_entities([TEST_MAT_VIEW])

    # Just making sure none of these raise an exception

    run_alembic_command(
        engine=engine,
        command="revision",
        command_kwargs={"autogenerate": True, "rev_id": "1", "message": "create"},
    )
