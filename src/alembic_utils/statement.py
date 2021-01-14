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
