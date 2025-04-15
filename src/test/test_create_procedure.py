from sqlalchemy import text

from alembic_utils.pg_procedure import PGProcedure

to_upper = PGProcedure(
    schema="public",
    signature="to_upper(some_text text)",
    definition="""
        as
        $$ begin execute format('set application_name= %L', UPPER(some_text)); end; $$ language plpgsql;
        """,
)


def test_create_and_drop(engine) -> None:
    """Test that the alembic current command does not error"""
    # Runs with no error
    up_sql = to_upper.to_sql_statement_create()
    down_sql = to_upper.to_sql_statement_drop()

    # Testing that the following two lines don't raise
    with engine.begin() as connection:
        connection.execute(up_sql)
        result = connection.execute(text("call public.to_upper('hello');"))
        assert result.rowcount == -1
        result = connection.execute(text("select current_setting('application_name');")).fetchone()
        assert result[0] == "HELLO"
        connection.execute(down_sql)
        assert True
