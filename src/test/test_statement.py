from alembic_utils.statement import coerce_to_quoted, coerce_to_unquoted, sanitize_python_identifier


def test_sanitize_python_identifier_with_hyphens() -> None:
    """Test that hyphens are replaced with underscores."""
    result = sanitize_python_identifier("reporting-readonly")
    assert "_" in result
    assert "-" not in result
    # Should be valid Python identifier
    assert result.isidentifier()


def test_sanitize_python_identifier_with_leading_digit() -> None:
    """Test that leading digits are handled by prepending underscore."""
    result = sanitize_python_identifier("123_role")
    assert result.isidentifier()
    assert result.startswith("_")


def test_sanitize_python_identifier_with_spaces() -> None:
    """Test that spaces are replaced with underscores."""
    result = sanitize_python_identifier("my role name")
    assert " " not in result
    assert result.isidentifier()


def test_sanitize_python_identifier_deterministic() -> None:
    """Test that the same input always produces the same output."""
    input_name = "reporting-readonly"
    result1 = sanitize_python_identifier(input_name)
    result2 = sanitize_python_identifier(input_name)
    assert result1 == result2


def test_sanitize_python_identifier_different_inputs_different_outputs() -> None:
    """Test that different inputs produce different outputs (collision avoidance)."""
    result1 = sanitize_python_identifier("role-a")
    result2 = sanitize_python_identifier("role_a")
    # These would collide without the hash suffix
    assert result1 != result2


def test_sanitize_python_identifier_special_characters() -> None:
    """Test various special characters are handled."""
    test_cases = [
        "role@domain",
        "role.name",
        "role:name",
        "role!name",
        "role#name",
    ]
    for name in test_cases:
        result = sanitize_python_identifier(name)
        assert result.isidentifier(), f"Failed for input: {name}, got: {result}"


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
