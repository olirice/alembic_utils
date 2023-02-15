from sqlalchemy import text

from alembic_utils.pg_function import PGFunction

to_upper = PGFunction(
    schema="public",
    signature="to_upper(some_text text)",
    definition="""
        returns text
        as
        $$ select upper(some_text) $$ language SQL;
        """,
)


def test_create_and_drop(engine) -> None:
    """Test that the alembic current command does not erorr"""
    # Runs with no error
    up_sql = to_upper.to_sql_statement_create()
    down_sql = to_upper.to_sql_statement_drop()

    # Testing that the following two lines don't raise
    with engine.begin() as connection:
        connection.execute(up_sql)
        result = connection.execute(text("select public.to_upper('hello');")).fetchone()
        assert result[0] == "HELLO"
        connection.execute(down_sql)
        assert True
