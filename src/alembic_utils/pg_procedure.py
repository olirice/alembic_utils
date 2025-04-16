# pylint: disable=unused-argument,invalid-name,line-too-long
from typing import List, Sequence, Type

from parse import parse
from sqlalchemy import text as sql_text
from sqlalchemy.orm import Session

from alembic_utils.exceptions import SQLParseFailure
from alembic_utils.replaceable_entity import ReplaceableEntity, T
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
    * **signature** - *str*: A SQL function's call signature
    * **definition** - *str*:  The remainig function body and identifiers
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
    def from_sql(cls: Type["PGProcedure"], sql: str) -> "PGProcedure":
        """Create an instance instance from a SQL string"""
        template = "create{}procedure{:s}{schema}.{signature}){:s}{definition}"
        result = parse(template, sql.strip(), case_sensitive=False)
        if result is not None:
            # remove possible quotes from signature
            raw_signature = result["signature"] + ')'
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
        """Adds quoting around the procedures name when emitting SQL statements

        e.g.
        'toUpper(text) returns text' -> '"toUpper"(text) returns text'
        """
        # May already be quoted if loading from database or SQL file
        name, remainder = self.signature.split("(", 1)
        return '"' + name.strip() + '"(' + remainder

    def to_sql_statement_create(self) -> sql_text:
        """Generates a SQL "create procedure" statement for PGProcedure"""
        return sql_text(
            f"CREATE PROCEDURE {self.literal_schema}.{self.literal_signature} {self.definition}"
        )

    def to_sql_statement_drop(self, cascade: bool = False) -> sql_text:
        """Generates a SQL "drop procedure" statement for PGProcedure"""
        template = "{function_name}({parameters})"
        result = parse(template, self.signature, case_sensitive=False)
        try:
            function_name = result["function_name"].strip()
            parameters_str = result["parameters"].strip()
        except TypeError:
            # Did not match, NoneType is not scriptable
            result = parse("{function_name}()", self.signature, case_sensitive=False)
            function_name = result["function_name"].strip()
            parameters_str = ""

        # NOTE: Will fail if a text field has a default and that deafult contains a comma...
        parameters: List[str] = parameters_str.split(",")
        parameters = [x[: len(x.lower().split("default")[0])] for x in parameters]
        parameters = [x.strip() for x in parameters]
        drop_params = ", ".join(parameters)
        return sql_text(
            f'DROP PROCEDURE {self.literal_schema}."{function_name}"({drop_params}) {"cascade" if cascade else ""}'
        )

    def to_sql_statement_create_or_replace(self) -> sql_text:
        """Generates a SQL "create or replace procedure" statement for PGProcedure"""
        yield sql_text(
            f"CREATE OR REPLACE PROCEDURE {self.literal_schema}.{self.literal_signature} {self.definition}"
        )

    @classmethod
    def from_database(
        cls: Type["PGProcedure"], sess: Session, schema="%"
    ) -> Sequence["PGProcedure"]:
        """Get a list of all procedures defined in the db"""
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
            and p.prokind = 'p'
        """
        )


        # Procedures are only supported in version 13 and greater
        db_functions: Sequence["PGProcedure"] = []
        if pg_version >= 130000:
            rows = sess.execute(sql, {"schema": schema}).fetchall()
            db_functions = [cls.from_sql(x[3]) for x in rows]

        for func in db_functions:
            assert func is not None

        return db_functions
