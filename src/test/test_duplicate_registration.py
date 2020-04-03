import pytest

from alembic_utils import TEST_VERSIONS_ROOT
from alembic_utils.exceptions import DuplicateRegistration
from alembic_utils.pg_function import PGFunction, register_functions
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


def test_migration_create_function(engine, reset: None) -> None:
    register_functions([TO_UPPER, TO_UPPER])

    with pytest.raises(DuplicateRegistration):
        run_alembic_command(
            engine=engine,
            command="revision",
            command_kwargs={"autogenerate": True, "rev_id": "1", "message": "raise"},
        )
