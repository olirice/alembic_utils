import pytest

from alembic_utils.pg_materialized_view import PGMaterializedView
from alembic_utils.pg_view import PGView
from alembic_utils.replaceable_entity import dependants, depends_on_expanded, register_entities, registry
from alembic_utils.testbase import TEST_VERSIONS_ROOT, run_alembic_command

# NAME_DEPENDENCIES inc. explicit depends_on
A = PGView(
    schema="public",
    signature="A_view",
    definition="select 1 as one",
    depends_on=[],
)

B_A = PGMaterializedView(
    schema="public",
    signature="B_view",
    definition='select * from public."A_view"',
    depends_on=[A],
)

C_A = PGView(
    schema="public",
    signature="C_view",
    definition='select * from public."A_view"',
    depends_on=[A],
)

D_B = PGView(
    schema="public",
    signature="D_view",
    definition='select * from public."B_view"',
    depends_on=[B_A],
)

E_AD = PGView(
    schema="public",
    signature="E_view",
    definition='select av.one, dv.one as two from public."A_view" as av join public."D_view" as dv on true',
    depends_on=[A, D_B],
)


@pytest.mark.parametrize("entity, result", [
    (A, set()),
    (B_A, {A}),
    (C_A, {A}),
    (D_B, {B_A, A}),
    (E_AD, {D_B, B_A, A}),
])
def test_depends_on_expanded(entity, result):
    assert depends_on_expanded(entity) == result


@pytest.mark.parametrize("entities, result", [
    ([A], {B_A, C_A, D_B, E_AD}),
    ([B_A], {D_B, E_AD}),
    ([C_A], set()),
    ([D_B], {E_AD}),
    ([E_AD], set()),
    ([E_AD, C_A], set()),
    ([B_A, C_A], {D_B, E_AD}),
    ([A, C_A], {B_A, C_A, D_B, E_AD}),
])
def test_dependants(entities, result):
    assert dependants(entities, [B_A, E_AD, D_B, C_A, A]) == result


def test_create_revision_with_explicit_depends_on(engine) -> None:
    register_entities([B_A, E_AD, D_B, C_A, A])

    run_alembic_command(
        engine=engine,
        command="revision",
        command_kwargs={"autogenerate": True, "rev_id": "1", "message": "create"},
    )
    # print(run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "1", "sql": True}))
    migration_create_path = TEST_VERSIONS_ROOT / "1_create.py"

    with migration_create_path.open() as migration_file:
        migration_contents = migration_file.read()

    assert migration_contents.count("op.create_entity") == 5
    assert migration_contents.count("op.drop_entity") == 5
    assert "op.replace_entity" not in migration_contents
    assert "from alembic_utils.pg_view import PGView" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})

    # part 2

    # update view
    D_B_mod = PGView(
        schema="public",
        signature="D_view",
        definition='select b.*, 1 as mod from public."B_view" as b',
        depends_on=[B_A]
    )
    E_AD.depends_on = [A, D_B_mod]  # update E's deps as D is new object

    registry.clear()
    register_entities([B_A, E_AD, D_B_mod, C_A, A])
    run_alembic_command(
        engine=engine,
        command="revision",
        command_kwargs={"autogenerate": True, "rev_id": "2", "message": "update"},
    )
    # print(run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "1:2", "sql": True}))
    migration_2_create_path = TEST_VERSIONS_ROOT / "2_update.py"

    with migration_2_create_path.open() as migration_file:
        migration_2_contents = migration_file.read()

    assert migration_2_contents.count("op.drop_entity") == 2
    assert migration_2_contents.count("op.replace_entity") == 2
    assert migration_2_contents.count("op.create_entity") == 2

    assert "from alembic_utils.pg_view import PGView" in migration_2_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})

    # print(run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "2:1", "sql": True}))
    # print(run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "1:base", "sql": True}))
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})
