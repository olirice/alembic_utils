def normalize_whitespace(text, base_whitespace: str = " ") -> str:
    """ Convert all whitespace to *base_whitespace* """
    return base_whitespace.join(text.split()).strip()


def strip_terminating_semicolon(sql: str) -> str:
    """Removes terminating semicolon on a SQL statement if it exists"""
    return sql.strip().rstrip(";").strip()
