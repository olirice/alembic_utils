from alembic_utils.statement import coerce_to_quoted, coerce_to_unquoted


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
