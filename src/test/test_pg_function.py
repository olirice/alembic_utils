import pytest

from alembic_utils.exceptions import SQLParseFailure
from alembic_utils.pg_function import PGFunction


def test_unparsable_body() -> None:

    invalid_function = PGFunction(
        schema="public",
        signature="to_upper(some_text text)",
        definition="""THIS IS NOT VALID SQL""",
    )

    with pytest.raises(SQLParseFailure):
        invalid_function.get_definition_body()
