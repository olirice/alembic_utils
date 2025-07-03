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


def test_create_and_drop(engine, execute_all) -> None:
    """Test that the alembic current command does not error"""
    # Runs with no error
    up_sql = to_upper.to_sql_statement_create()
    down_sql = to_upper.to_sql_statement_drop()

    # Testing that the following two lines don't raise
    with engine.begin() as connection:
        execute_all(connection, up_sql)
        result = connection.execute(text("select public.to_upper('hello');")).fetchone()
        assert result[0] == "HELLO"
        execute_all(connection, down_sql)
        assert True
