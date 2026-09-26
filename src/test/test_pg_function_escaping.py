import pytest
from sqlalchemy import text

from alembic_utils.pg_function import PGFunction
from alembic_utils.replaceable_entity import register_entities
from alembic_utils.testbase import run_alembic_command


@pytest.mark.parametrize(
    "language, expression, expected",
    [
        (language, expression, expected)
        for language in ["sql", "'sql'", "plpgsql", "'plpgsql'"]
        for expression, expected in [
            ("to_char(make_timestamp(2026, 1, 2, 13, 45, 0), 'HH24:MI')", "13:45"),
            ("':name'::text", ":name"),
            ("'https://example.test/a::b'::text", "https://example.test/a::b"),
            ("':='::text", ":="),
            ("'trailing:'::text", "trailing:"),
        ]
    ]
    + [
        # SQL function bodies must retain intentional backslashes through rendering.
        ("sql", r"'\:name'::text", r"\:name"),
        ("'sql'", r"'\:name'::text", r"\:name"),
        ("sql", r"'\\:name'::text", r"\\:name"),
        ("'sql'", r"'\\:name'::text", r"\\:name"),
        # Existing generated PL/pgSQL migrations contain SQLAlchemy colon escapes.
        ("plpgsql", r"'HH24\:MI'::text", "HH24:MI"),
        ("'plpgsql'", r"'HH24\:MI'::text", "HH24:MI"),
    ],
)
def test_function_escaping_survives_migrations(engine, language, expression, expected):
    def function(value):
        if "plpgsql" in language:
            body = f"DECLARE result text; BEGIN result := {value}; RETURN result; END;"
        else:
            body = f"SELECT {value}"
        return PGFunction(
            schema="public",
            signature="colon_round_trip()",
            definition=f"RETURNS text AS $$ {body} $$ LANGUAGE {language}",
        )

    def assert_result(value):
        with engine.connect() as connection:
            assert connection.execute(text("SELECT public.colon_round_trip()")).scalar() == value

    original = function(expression)
    # Creation without rendering must preserve the same independently stated result.
    with engine.begin() as connection:
        connection.execute(original.to_sql_statement_create())
    assert_result(expected)
    with engine.begin() as connection:
        connection.execute(original.to_sql_statement_drop())

    register_entities([original], entity_types=[PGFunction])
    run_alembic_command(
        engine, "revision", {"autogenerate": True, "rev_id": "1", "message": "create"}
    )
    run_alembic_command(engine, "upgrade", {"revision": "head"})
    assert_result(expected)
    run_alembic_command(engine, "check", {})

    updated = function(f"({expression}) || ' updated'")
    register_entities([updated], entity_types=[PGFunction])
    run_alembic_command(
        engine, "revision", {"autogenerate": True, "rev_id": "2", "message": "replace"}
    )
    run_alembic_command(engine, "upgrade", {"revision": "head"})
    assert_result(expected + " updated")
    run_alembic_command(engine, "check", {})

    # The downgrade is rendered from the reflected, previously installed definition.
    run_alembic_command(engine, "downgrade", {"revision": "1"})
    assert_result(expected)
    run_alembic_command(engine, "downgrade", {"revision": "base"})
    run_alembic_command(engine, "upgrade", {"revision": "head"})
    assert_result(expected + " updated")
    register_entities([updated], entity_types=[PGFunction])
    run_alembic_command(engine, "check", {})
    run_alembic_command(engine, "downgrade", {"revision": "base"})
