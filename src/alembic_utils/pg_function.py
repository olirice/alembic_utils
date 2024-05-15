# pylint: disable=unused-argument,invalid-name,line-too-long
import logging

from sqlalchemy import text as sql_text

from alembic_utils.replaceable_entity import ReplaceableEntity
from alembic_utils.statement import (
    escape_colon_for_plpgsql,
    escape_colon_for_sql,
    normalize_whitespace,
    strip_terminating_semicolon,
    render_drop_statement,
    split_function,
)


logger = logging.getLogger(__name__)


class PGFunction(ReplaceableEntity):
    """A PostgreSQL Function compatible with `alembic revision --autogenerate`

    **Parameters:**

    * **schema** - *str*: A SQL schema name
    * **signature** - *str*: A SQL function's call signature
    * **definition** - *str*:  The remainig function body and identifiers
    """

    type_ = "function"

    def __init__(self, schema: str, signature: str, definition: str, is_proc=False):
        super().__init__(schema, signature, definition)
        # Detect if function uses plpgsql and update escaping rules to not escape ":="
        is_plpgsql: bool = "language plpgsql" in normalize_whitespace(definition).lower().replace(
            "'", ""
        )
        escaping_callable = escape_colon_for_plpgsql if is_plpgsql else escape_colon_for_sql
        # Override definition with correct escaping rules
        self.is_proc = is_proc
        self.definition: str = escaping_callable(strip_terminating_semicolon(definition))

    @classmethod
    def from_sql(cls, sql: str) -> "PGFunction":
        """Create an instance instance from a SQL string"""
        signature, returns, schema, body, is_proc = split_function(sql)

        return cls(
            schema=schema,
            signature=signature,
            definition=f"{returns} {body}".strip(),
            is_proc=is_proc,
        )

    @property
    def literal_signature(self) -> str:
        """Adds quoting around the functions name when emitting SQL statements

        e.g.
        'toUpper(text) returns text' -> '"toUpper"(text) returns text'
        """
        # May already be quoted if loading from database or SQL file
        name, remainder = self.signature.split("(", 1)
        return '"' + name + '"(' + remainder

    def render_self_for_migration(self, omit_definition=False, **kwargs) -> str:
        arg_to_value_init = {
            "is_proc": self.is_proc,
        }
        return super().render_self_for_migration(omit_definition=omit_definition, arg_to_value=arg_to_value_init, **kwargs)

    def to_sql_statement_create(self):
        """Generates a SQL "create function" statement for PGFunction"""
        entity_type = "PROCEDURE" if self.is_proc else "FUNCTION"
        return sql_text(
            f"CREATE {entity_type} {self.literal_schema}.{self.literal_signature} {self.definition}"
        )

    def to_sql_statement_drop(self, cascade=False):
        """Generates a SQL "drop function/procedure" statement for PGFunction"""
        entity_kind = "PROCEDURE" if self.is_proc else "FUNCTION"
        full_sql = f"CREATE {entity_kind} {self.literal_schema}.{self.literal_signature} {self.definition}"
        drop_stmt = render_drop_statement(full_sql, self.is_proc)
        if cascade:
            drop_stmt += " cascade"
        return sql_text(drop_stmt)

    def to_sql_statement_create_or_replace(self):
        """Generates a SQL "create or replace function/procedure" statement for PGFunction"""
        entity_type = "PROCEDURE" if self.is_proc else "FUNCTION"
        yield sql_text(
            f"CREATE OR REPLACE {entity_type} {self.literal_schema}.{self.literal_signature} {self.definition}"
        )

    @classmethod
    def from_database(cls, sess, schema):
        """Get a list of all functions defined in the db"""

        # Prior to postgres 11, pg_proc had different columns
        # https://github.com/olirice/alembic_utils/issues/12
        PG_GTE_11 = """
            and p.prokind in ('f', 'p')
        """

        PG_LT_11 = """
            and not p.proisagg
            and not p.proiswindow
        """

        # Retrieve the postgres server version e.g. 90603 for 9.6.3 or 120003 for 12.3
        pg_version_str = sess.execute(sql_text("show server_version_num")).fetchone()[0]
        pg_version = int(pg_version_str)

        sql = sql_text(
            """
        with extension_functions as (
            select
                objid as extension_function_oid
            from
                pg_depend
            where
                -- depends on an extension
                deptype='e'
                -- is a proc/function
                and classid = 'pg_proc'::regclass
        )

        select
            n.nspname as function_schema,
            p.proname as function_name,
            pg_get_function_arguments(p.oid) as function_arguments,
            case
                when l.lanname = 'internal' then p.prosrc
                else pg_get_functiondef(p.oid)
            end as create_statement,
            t.typname as return_type,
            l.lanname as function_language
        from
            pg_proc p
            left join pg_namespace n on p.pronamespace = n.oid
            left join pg_language l on p.prolang = l.oid
            left join pg_type t on t.oid = p.prorettype
            left join extension_functions ef on p.oid = ef.extension_function_oid
        where
            n.nspname not in ('pg_catalog', 'information_schema')
            -- Filter out functions from extensions
            and ef.extension_function_oid is null
            and n.nspname = :schema
        """
            + (PG_GTE_11 if pg_version >= 110000 else PG_LT_11)
        )

        rows = sess.execute(sql, {"schema": schema}).fetchall()
        db_functions = [cls.from_sql(x[3]) for x in rows]

        for func in db_functions:
            assert func is not None

        return db_functions
