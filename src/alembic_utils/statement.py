import logging
from uuid import uuid4

import pglast
import pglast.ast
from pglast.stream import RawStream, IndentedStream
from parse import parse

from alembic_utils.exceptions import SQLParseFailure


logger = logging.getLogger(__name__)


def _get_func_signature_returns_and_type(func_sql: str):
    """
    return the function signature, returns clause, and whether it is a procedure
    """
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("raw sql: %s", func_sql)
        parse_tree_json = pglast.parser.parse_sql_json(func_sql)
        logger.debug("parse_tree_json: %s", parse_tree_json)

    parse_tree = pglast.parser.parse_sql(func_sql)
    if len(parse_tree) != 1:
        raise ValueError(f"Expected 1 statement, got {len(parse_tree)}")

    stmt = parse_tree[0].stmt

    if not isinstance(stmt, pglast.ast.CreateFunctionStmt):
        raise ValueError(f"Expected CreateFunctionStmt got {stmt}")

    is_proc = stmt.is_procedure

    stmt.is_procedure = False
    stmt.replace = False
    stmt.options = []
    stmt.sql_body = None

    if len(stmt.funcname) > 1:
        # Remove schema name
        stmt.funcname = stmt.funcname[1:]

    stmt_str = RawStream()(parse_tree[0])

    prefix = "create function"
    if not stmt_str.lower().startswith(prefix):
        raise ValueError(f"Expected {prefix} got {stmt_str}")

    stmt_str = stmt_str[len(prefix) :].strip()
    return stmt_str, is_proc


def _get_func_schema(func_sql: str, default="public") -> str:
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("raw sql: %s", func_sql)
        parse_tree_json = pglast.parser.parse_sql_json(func_sql)
        logger.debug("parse_tree_json: %s", parse_tree_json)

    parse_tree = pglast.parser.parse_sql(func_sql)
    if len(parse_tree) != 1:
        raise ValueError(f"Expected 1 statement, got {len(parse_tree)}")

    stmt = parse_tree[0].stmt

    if not isinstance(stmt, pglast.ast.CreateFunctionStmt):
        raise ValueError(f"Expected CreateFunctionStmt got {stmt}")

    if len(stmt.funcname) > 1:
        schema_name = RawStream()(stmt.funcname[0])
        if schema_name.startswith('"') or schema_name.startswith("'"):
            schema_name = schema_name[1:-1]
        return schema_name
    return default


def _get_func_body(func_sql: str) -> str:
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("raw sql: %s", func_sql)
        parse_tree_json = pglast.parser.parse_sql_json(func_sql)
        logger.debug("parse_tree_json: %s", parse_tree_json)

    dummy_func_sql = "CREATE FUNCTION foo.bar() RETURNS void AS $$ BEGIN $$ LANGUAGE plpgsql;"
    parse_tree_dummy = pglast.parser.parse_sql(dummy_func_sql)

    parse_tree = pglast.parser.parse_sql(func_sql)
    if len(parse_tree) != 1:
        raise ValueError(f"Expected 1 statement, got {len(parse_tree)}")

    stmt = parse_tree[0].stmt

    if not isinstance(stmt, pglast.ast.CreateFunctionStmt):
        raise ValueError(f"Expected CreateFunctionStmt got {stmt}")

    stmt.replace = False
    stmt.is_procedure = False
    stmt.funcname = parse_tree_dummy[0].stmt.funcname
    stmt.parameters = parse_tree_dummy[0].stmt.parameters
    stmt.returnType = parse_tree_dummy[0].stmt.returnType

    func_str = IndentedStream(comma_at_eoln=True)(parse_tree)
    func_split = func_str.split("RETURNS void")
    if len(func_split) != 2:
        raise ValueError(f"Expected 1 instance of RETURNS void in statement: {func_sql}")
    if func_split[0].strip().lower() != "create function foo.bar()":
        raise ValueError(f"Expected CREATE FUNCTION foo.bar() got {func_split[0]}")
    return func_split[1]


def validate_split_function(signature, returns, schema, body, is_proc):
    entity_kind = "PROCEDURE" if is_proc else "FUNCTION"
    reconstructed_sql = f"CREATE {entity_kind} {schema}.{signature} {returns} {body}"
    pglast.parser.parse_sql(reconstructed_sql)


def split_function(sql: str):
    """
    split a function or procedure into: signature, returns clause, schema, body, and whether it is a procedure
    """
    try:
        signature_and_return, is_proc = _get_func_signature_returns_and_type(sql)
        template = "{signature}returns{ret_type}"
        result = parse(template, signature_and_return, case_sensitive=False)
        if result is None:
            raw_signature = signature_and_return
            returns = ""
        else:
            raw_signature = result["signature"].strip()
            returns = f"returns {result['ret_type'].strip()}"

        schema = _get_func_schema(sql)
        body = _get_func_body(sql)

        # remove possible quotes from signature
        signature = "".join(raw_signature.split('"', 2)) if raw_signature.startswith('"') else raw_signature

        validate_split_function(signature, returns, schema, body, is_proc)
        return signature, returns, schema, body, is_proc
    except pglast.parser.ParseError as e:
        raise SQLParseFailure(str(e)) from e


def render_drop_statement(func_sql: str, is_proc: bool) -> str:
    entity_kind = "PROCEDURE" if is_proc else "FUNCTION"

    dummy_drop_sql = f"DROP {entity_kind} sqrt(integer);"
    parse_tree = pglast.parser.parse_sql(dummy_drop_sql)
    assert len(parse_tree) == 1
    stmt = parse_tree[0].stmt
    assert isinstance(stmt, pglast.ast.DropStmt)
    assert len(stmt.objects) == 1
    obj = stmt.objects[0]

    parse_tree2 = pglast.parser.parse_sql(func_sql)
    assert len(parse_tree2) == 1
    stmt2 = parse_tree2[0].stmt
    assert isinstance(stmt2, pglast.ast.CreateFunctionStmt)

    obj.objname = stmt2.funcname
    obj.objargs = []
    obj.objfuncargs = stmt2.parameters
    for arg in obj.objfuncargs or []:
        arg.defexpr = None

    return RawStream()(parse_tree)


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
