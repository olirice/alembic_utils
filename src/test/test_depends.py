import pytest

from alembic_utils.depends import solve_resolution_order
from alembic_utils.pg_view import PGView
from alembic_utils.replaceable_entity import register_entities
from alembic_utils.testbase import TEST_VERSIONS_ROOT, run_alembic_command

# NAME_DEPENDENCIES

A = PGView(
    schema="public",
    signature="A_view",
    definition="select 1 as one",
)

B_A = PGView(
    schema="public",
    signature="B_view",
    definition='select * from public."A_view"',
)

C_A = PGView(
    schema="public",
    signature="C_view",
    definition='select * from public."A_view"',
)

D_B = PGView(
    schema="public",
    signature="D_view",
    definition='select * from public."B_view"',
)

E_AD = PGView(
    schema="public",
    signature="E_view",
    definition='select av.one, dv.one as two from public."A_view" as av join public."D_view" as dv on true',
)


@pytest.mark.parametrize(
    "order",
    [
        [A, B_A, C_A, D_B, E_AD],
        [C_A, A, B_A, E_AD, D_B],
        [B_A, C_A, A, D_B, E_AD],
        [B_A, E_AD, D_B, C_A, A],
    ],
)
def test_solve_resolution_order(sess, order) -> None:
    solution = solve_resolution_order(sess, order)

    assert solution.index(A) < solution.index(B_A)
    assert solution.index(A) < solution.index(C_A)
    assert solution.index(B_A) < solution.index(D_B)
    assert solution.index(A) < solution.index(E_AD)
    assert solution.index(D_B) < solution.index(E_AD)


def test_create_revision(engine) -> None:
    register_entities([B_A, E_AD, D_B, C_A, A], entity_types=[PGView])

    output = run_alembic_command(
        engine=engine,
        command="revision",
        command_kwargs={"autogenerate": True, "rev_id": "1", "message": "create"},
    )

    migration_create_path = TEST_VERSIONS_ROOT / "1_create.py"

    with migration_create_path.open() as migration_file:
        migration_contents = migration_file.read()

    assert migration_contents.count("op.create_entity") == 5
    assert migration_contents.count("op.drop_entity") == 5
    assert "op.replace_entity" not in migration_contents
    assert "from alembic_utils.pg_view import PGView" in migration_contents

    # Execute upgrade
    run_alembic_command(engine=engine, command="upgrade", command_kwargs={"revision": "head"})
    # Execute Downgrade
    run_alembic_command(engine=engine, command="downgrade", command_kwargs={"revision": "base"})
