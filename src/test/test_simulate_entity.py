import pytest
from sqlalchemy import text
from sqlalchemy.exc import DataError
from sqlalchemy.orm import Session

from alembic_utils.pg_view import PGView
from alembic_utils.simulate import simulate_entity

TEST_VIEW = PGView(
    schema="public",
    signature="tview",
    definition="select *, FALSE as is_updated from pg_views",
)


def test_simulate_entity_shows_user_code_error(sess: Session) -> None:
    sess.execute(TEST_VIEW.to_sql_statement_create())

    with pytest.raises(DataError):
        with simulate_entity(sess, TEST_VIEW):
            # Raises a sql error
            sess.execute(text("select 1/0")).fetchone()

    # Confirm context manager exited gracefully
    assert True
