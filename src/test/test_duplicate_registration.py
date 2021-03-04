import pytest

from alembic_utils.exceptions import DuplicateRegistration
from alembic_utils.pg_function import PGFunction
from alembic_utils.replaceable_entity import register_entities
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


def test_migration_create_function(engine) -> None:
    register_entities([TO_UPPER, TO_UPPER], entity_types=[PGFunction])

    with pytest.raises(DuplicateRegistration):
        run_alembic_command(
            engine=engine,
            command="revision",
            command_kwargs={"autogenerate": True, "rev_id": "1", "message": "raise"},
        )
