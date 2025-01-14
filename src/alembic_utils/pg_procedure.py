# pylint: disable=unused-argument,invalid-name,line-too-long
from typing import List

from parse import parse
from sqlalchemy import text as sql_text

from alembic_utils.exceptions import SQLParseFailure
from alembic_utils.replaceable_entity import ReplaceableEntity
from alembic_utils.statement import (
    escape_colon_for_plpgsql,
    escape_colon_for_sql,
    normalize_whitespace,
    strip_terminating_semicolon,
)


class PGProcedure(ReplaceableEntity):
    """A PostgreSQL Procedure compatible with `alembic revision --autogenerate`

    **Parameters:**

    * **schema** - *str*: A SQL schema name
    * **signature** - *str*: A SQL procedure's call signature
    * **definition** - *str*:  The remainig procedure body and identifiers
    """

    type_ = "procedure"

    def __init__(self, schema: str, signature: str, definition: str):
        super().__init__(schema, signature, definition)
        # Detect if procedure uses plpgsql and update escaping rules to not escape ":="
        is_plpgsql: bool = "language plpgsql" in normalize_whitespace(definition).lower().replace(
            "'", ""
        )
        escaping_callable = escape_colon_for_plpgsql if is_plpgsql else escape_colon_for_sql
        # Override definition with correct escaping rules
        self.definition: str = escaping_callable(strip_terminating_semicolon(definition))

    @classmethod
    def from_sql(cls, sql: str) -> "PGProcedure":
        """Create an instance instance from a SQL string"""
        template = "create{}procedure{:s}{schema}.{signature_name}({signature_arg}){:s}{definition}"
        result = parse(template, sql.strip(), case_sensitive=False)
        if result is not None:
            raw_signature = f'{result["signature_name"]}({result["signature_arg"]})'
            # remove possible quotes from signature
            signature = (
                "".join(raw_signature.split('"', 2))
                if raw_signature.startswith('"')
                else raw_signature
            )
            return cls(
                schema=result["schema"],
                signature=signature,
                definition=result["definition"],
            )
        raise SQLParseFailure(f'Failed to parse SQL into PGProcedure """{sql}"""')

    @property
    def literal_signature(self) -> str:
        """Adds quoting around the procedure name when emitting SQL statements

        e.g.
        'toUpper(text) returns text' -> '"toUpper"(text) text'
        """
        # May already be quoted if loading from database or SQL file
        name, remainder = self.signature.split("(", 1)
        return '"' + name.strip() + '"(' + remainder

    def to_sql_statement_create(self):
        """Generates a SQL "create procedure" statement for PGProcedure"""
        return sql_text(
            f"CREATE PROCEDURE {self.literal_schema}.{self.literal_signature} {self.definition}",
        )

    def to_sql_statement_drop(self, cascade=False):
        """Generates a SQL "drop procedure" statement for PGProcedure"""
        cascade = "cascade" if cascade else ""
        template = "{procedure_name}({parameters})"
        result = parse(template, self.signature, case_sensitive=False)
        try:
            procedure_name = result["procedure_name"].strip()
            parameters_str = result["parameters"].strip()
        except TypeError:
            # Did not match, NoneType is not scriptable
            result = parse("{procedure_name}()", self.signature, case_sensitive=False)
            procedure_name = result["procedure_name"].strip()
            parameters_str = ""

        # NOTE: Will fail if a text field has a default and that deafult contains a comma...
        parameters: list[str] = parameters_str.split(",")
        parameters = [x[: len(x.lower().split("default")[0])] for x in parameters]
        parameters = [x.strip() for x in parameters]
        drop_params = ", ".join(parameters)
        return sql_text(
            f'DROP PROCEDURE {self.literal_schema}."{procedure_name}"({drop_params}) {cascade}',
        )

    def to_sql_statement_create_or_replace(self):
        """Generates a SQL "create or replace procedure" statement for PGProcedure"""
        yield sql_text(
            f"CREATE OR REPLACE PROCEDURE {self.literal_schema}.{self.literal_signature} {self.definition}",
        )

    @classmethod
    def from_database(cls, sess, schema):
        """Get a list of all procedures defined in the db"""

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
            p.proname as procedure_name,
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
            and p.prokind = 'p'
        """
        )

        rows = sess.execute(sql, {"schema": schema}).fetchall()
        db_functions = [cls.from_sql(x[3]) for x in rows]

        for func in db_functions:
            assert func is not None

        return db_functions
