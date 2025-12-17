import hashlib
import re
from uuid import uuid4


def sanitize_python_identifier(name: str) -> str:
    """Sanitize a string to be a valid Python identifier.

    - Replaces invalid characters (hyphens, spaces, etc.) with underscores
    - Ensures the identifier does not start with a digit
    - Appends a short deterministic hash to avoid collisions from sanitization

    Examples:
        sanitize_python_identifier('reporting-readonly') => 'reporting_readonly_a1b2c3'
        sanitize_python_identifier('123_role') => '_123_role_d4e5f6'
        sanitize_python_identifier('valid_name') => 'valid_name_g7h8i9'
    """
    original = name
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    if sanitized and sanitized[0].isdigit():
        sanitized = "_" + sanitized
    hash_suffix = hashlib.md5(original.encode()).hexdigest()[:6]
    return f"{sanitized}_{hash_suffix}"


def normalize_whitespace(text, base_whitespace: str = " ") -> str:
    """Convert all whitespace to *base_whitespace*"""
    return base_whitespace.join(text.split()).strip()


def strip_terminating_semicolon(sql: str) -> str:
    """Removes terminating semicolon on a SQL statement if it exists"""
    return sql.strip().rstrip(";").strip()


def strip_double_quotes(sql: str) -> str:
    """Removes starting and ending double quotes"""
    sql = sql.strip().rstrip('"')
    return sql.strip().lstrip('"').strip()


def escape_colon_for_sql(sql: str) -> str:
    """Escapes colons for use in sqlalchemy.text"""
    holder = str(uuid4())
    sql = sql.replace("::", holder)
    sql = sql.replace(":", r"\:")
    sql = sql.replace(holder, "::")
    return sql


def escape_colon_for_plpgsql(sql: str) -> str:
    """Escapes colons for plpgsql for use in sqlalchemy.text"""
    holder1 = str(uuid4())
    holder2 = str(uuid4())
    holder3 = str(uuid4())
    sql = sql.replace("::", holder1)
    sql = sql.replace(":=", holder2)
    sql = sql.replace(r"\:", holder3)

    sql = sql.replace(":", r"\:")

    sql = sql.replace(holder3, r"\:")
    sql = sql.replace(holder2, ":=")
    sql = sql.replace(holder1, "::")
    return sql


def coerce_to_quoted(text: str) -> str:
    """Coerces schema and entity names to double quoted one

    Examples:
        coerce_to_quoted('"public"') => '"public"'
        coerce_to_quoted('public') => '"public"'
        coerce_to_quoted('public.table') => '"public"."table"'
        coerce_to_quoted('"public".table') => '"public"."table"'
        coerce_to_quoted('public."table"') => '"public"."table"'
    """
    if "." in text:
        schema, _, name = text.partition(".")
        schema = f'"{strip_double_quotes(schema)}"'
        name = f'"{strip_double_quotes(name)}"'
        return f"{schema}.{name}"

    text = strip_double_quotes(text)
    return f'"{text}"'


def coerce_to_unquoted(text: str) -> str:
    """Coerces schema and entity names to unquoted

    Examples:
        coerce_to_unquoted('"public"') => 'public'
        coerce_to_unquoted('public') => 'public'
        coerce_to_unquoted('public.table') => 'public.table'
        coerce_to_unquoted('"public".table') => 'public.table'
    """
    return "".join(text.split('"'))
