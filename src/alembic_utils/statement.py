from uuid import uuid4


def normalize_whitespace(text, base_whitespace: str = " ") -> str:
    """ Convert all whitespace to *base_whitespace* """
    return base_whitespace.join(text.split()).strip()


def strip_terminating_semicolon(sql: str) -> str:
    """Removes terminating semicolon on a SQL statement if it exists"""
    return sql.strip().rstrip(";").strip()


def strip_double_quotes(sql: str) -> str:
    """Removes starting and ending double quotes"""
    sql = sql.strip().rstrip('"')
    return sql.strip().lstrip('"').strip()


def escape_colon(sql: str) -> str:
    """Escapes colons for for use in sqlalchemy.text"""
    holder = str(uuid4())
    sql = sql.replace("::", holder)
    sql = sql.replace(":", "\:")
    sql = sql.replace(holder, "::")
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
