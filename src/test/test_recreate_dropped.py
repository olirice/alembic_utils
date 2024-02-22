import pytest
from sqlalchemy.orm import Session

from alembic_utils.depends import recreate_dropped
from alembic_utils.pg_view import PGView

TEST_ROOT_BIGINT = PGView(
    schema="public", signature="root", definition="select 1::bigint as some_val"
)

TEST_ROOT_INT = PGView(schema="public", signature="root", definition="select 1::int as some_val")

TEST_DEPENDENT = PGView(schema="public", signature="branch", definition="select * from public.root")


def test_fails_without_defering(sess: Session) -> None:

    # Create the original view
    sess.execute(TEST_ROOT_BIGINT.to_sql_statement_create())
    # Create the view that depends on it
    sess.execute(TEST_DEPENDENT.to_sql_statement_create())

    # Try to update a column type of the base view from undeneath
    # the dependent view
    with pytest.raises(Exception):
        sess.execute(TEST_ROOT_INT.to_sql_statement_create_or_replace())


def test_succeeds_when_defering(engine) -> None:

    with engine.begin() as connection:
        # Create the original view
        connection.execute(TEST_ROOT_BIGINT.to_sql_statement_create())
        # Create the view that depends on it
        connection.execute(TEST_DEPENDENT.to_sql_statement_create())

    # Try to update a column type of the base view from undeneath
    # the dependent view
    with recreate_dropped(connection=engine) as sess:
        sess.execute(TEST_ROOT_INT.to_sql_statement_drop(cascade=True))
        sess.execute(TEST_ROOT_INT.to_sql_statement_create())


def test_fails_gracefully_on_bad_user_statement(engine) -> None:
    with engine.begin() as connection:
        # Create the original view
        connection.execute(TEST_ROOT_BIGINT.to_sql_statement_create())
        # Create the view that depends on it
        connection.execute(TEST_DEPENDENT.to_sql_statement_create())

    # Execute a failing statement in the session
    with pytest.raises(Exception):
        with recreate_dropped(connection=engine) as sess:
            sess.execute(TEST_ROOT_INT.to_sql_statement_create())


def test_fails_if_user_creates_new_entity(engine) -> None:
    with engine.begin() as connection:
        # Create the original view
        connection.execute(TEST_ROOT_BIGINT.to_sql_statement_create())

    # User creates a brand new entity
    with pytest.raises(Exception):
        with recreate_dropped(connection=connection) as sess:
            connection.execute(TEST_DEPENDENT.to_sql_statement_create())
