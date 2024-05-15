from dataclasses import dataclass
import pytest

from alembic_utils.statement import (
    coerce_to_quoted,
    coerce_to_unquoted,
    split_function,
    render_drop_statement,
    normalize_whitespace,
)


@dataclass
class FuncTestCase:
    name: str
    sql: str
    expected_signature: str
    expected_returns: str
    expected_schema: str
    expected_body: str
    expected_is_proc: bool
    expected_drop_stmt: str
    allow_error: bool


_TEST_CASES = [
    FuncTestCase(
        name="internal function",
        sql="""
CREATE FUNCTION square_root(double precision) RETURNS double precision
AS 'dsqrt' LANGUAGE internal STRICT;
""",
        expected_signature="square_root(double precision)",
        expected_returns="returns double precision",
        expected_schema="public",
        # note: STRICT is the same as RETURNS NULL ON NULL INPUT. Is there a way to preserve the original keyword?
        expected_body="AS $$dsqrt$$ LANGUAGE internal RETURNS NULL ON NULL INPUT",
        expected_is_proc=False,
        expected_drop_stmt="DROP FUNCTION square_root (double precision)",
        allow_error=False,
    ),
    FuncTestCase(
        name="function with default, OUT paramter, and no RETURNS",
        sql="""
CREATE OR REPLACE FUNCTION func_w_default(in_fields text[] = ARRAY['foo', 'bar'], OUT data jsonb)
language 'plpgsql' as $func$ SELECT 1; $func$
""",
        expected_signature="func_w_default(in_fields text[] = ARRAY['foo', 'bar'], OUT data jsonb)",
        expected_returns="",
        expected_schema="public",
        expected_body="LANGUAGE plpgsql AS $$ SELECT 1; $$",
        expected_is_proc=False,
        expected_drop_stmt="DROP FUNCTION func_w_default (in_fields text[], OUT data jsonb)",
        allow_error=False,
    ),
    FuncTestCase(
        name="procedure",
        sql="""
CREATE OR REPLACE PROCEDURE myschema.clear_cache(obj_id uuid)
LANGUAGE plpgsql
AS $$
DECLARE
BEGIN
    CALL clear_parent(obj_id, true);
END;
$$
""",
        expected_signature="clear_cache(obj_id uuid)",
        expected_returns="",
        expected_schema="myschema",
        expected_body="LANGUAGE plpgsql AS $$ DECLARE BEGIN CALL clear_parent(obj_id, true); END; $$",
        expected_is_proc=True,
        expected_drop_stmt="DROP PROCEDURE myschema.clear_cache (obj_id uuid)",
        allow_error=False,
    ),
    FuncTestCase(
        name="begin atomic",
        sql="""
CREATE OR REPLACE FUNCTION select_int(n integer)
  RETURNS integer
  LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
BEGIN ATOMIC
    RETURN n;
END
""",
        expected_signature="select_int(n integer)",
        expected_returns="returns integer",
        expected_schema="public",
        expected_body="LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE BEGIN ATOMIC RETURN n; END",
        expected_is_proc=False,
        expected_drop_stmt="",
        allow_error=True,  # BEGIN ATOMIC not yet supported by pglast
    ),
]


@pytest.mark.parametrize("data", _TEST_CASES, ids=[tc.name for tc in _TEST_CASES])
def test_split_function(data: FuncTestCase) -> None:
    if data.allow_error:
        pytest.xfail("This test is expected to fail")

    signature, returns, schema, body, is_proc = split_function(data.sql)
    assert is_proc == data.expected_is_proc
    assert render_drop_statement(data.sql, is_proc) == data.expected_drop_stmt
    assert signature == data.expected_signature
    assert returns == data.expected_returns
    assert schema == data.expected_schema
    assert normalize_whitespace(body) == data.expected_body


def test_coerce_to_quoted() -> None:
    assert coerce_to_quoted('"public"') == '"public"'
    assert coerce_to_quoted("public") == '"public"'
    assert coerce_to_quoted("public.table") == '"public"."table"'
    assert coerce_to_quoted('"public".table') == '"public"."table"'
    assert coerce_to_quoted('public."table"') == '"public"."table"'


def test_coerce_to_unquoted() -> None:
    assert coerce_to_unquoted('"public"') == "public"
    assert coerce_to_unquoted("public") == "public"
    assert coerce_to_unquoted("public.table") == "public.table"
    assert coerce_to_unquoted('"public".table') == "public.table"
