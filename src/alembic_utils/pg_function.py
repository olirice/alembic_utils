# pylint: disable=unused-argument,invalid-name,line-too-long
from __future__ import annotations

from typing import List

from parse import parse
from sqlalchemy import text as sql_text

from alembic_utils.exceptions import SQLParseFailure
from alembic_utils.replaceable_entity import ReplaceableEntity


class PGFunction(ReplaceableEntity):
    """A PostgreSQL Function compatible with `alembic revision --autogenerate`

    **Parameters:**

    * **schema** - *str*: A SQL schema name
    * **signature** - *str*: A SQL function's call signature
    * **definition** - *str*:  The remainig function body and identifiers

    """

    @classmethod
    def from_sql(cls, sql: str) -> PGFunction:
        """Create an instance instance from a SQL string"""
        template = "create{}function{:s}{schema}.{signature}{:s}returns{:s}{definition}"
        result = parse(template, sql.strip(), case_sensitive=False)
        if result is not None:
            return cls(
                schema=result["schema"],
                signature=result["signature"],
                definition="returns " + result["definition"],
            )
        raise SQLParseFailure(f'Failed to parse SQL into PGFunction """{sql}"""')

    def to_sql_statement_create(self) -> str:
        """ Generates a SQL "create function" statement for PGFunction """
        return sql_text(f"CREATE FUNCTION {self.schema}.{self.signature} {self.definition}")

    def to_sql_statement_drop(self) -> str:
        """ Generates a SQL "drop function" statement for PGFunction """
        template = "{function_name}({parameters})"
        # Rmove default clause from drop statements because they don't parse
        result = parse(template, self.signature, case_sensitive=False)
        function_name = result["function_name"]
        parameters_str = result["parameters"].strip()
        # NOTE: Will fail if a text field has a default and that deafult contains a comma...
        parameters: List[str] = parameters_str.split(",")
        parameters = [x[: len(x.lower().split("default")[0])] for x in parameters]
        parameters = [x.strip() for x in parameters]
        drop_params = ", ".join(parameters)
        return sql_text(f"DROP FUNCTION {self.schema}.{function_name}({drop_params})")

    def to_sql_statement_create_or_replace(self) -> str:
        """ Generates a SQL "create or replace function" statement for PGFunction """
        return sql_text(
            f"CREATE OR REPLACE FUNCTION {self.schema}.{self.signature} {self.definition}"
        )

    @classmethod
    def from_database(cls, connection, schema="%") -> List[PGFunction]:
        """Get a list of all functions defined in the db"""
        sql = sql_text(
            f"""
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
        where
            n.nspname not in ('pg_catalog', 'information_schema')
            and n.nspname like '{schema}';
        """
        )
        rows = connection.execute(sql).fetchall()
        db_functions = [PGFunction.from_sql(x[3]) for x in rows]

        for func in db_functions:
            assert func is not None

        return db_functions

    def get_compare_identity_query(self):
        """Only called in simulation. alembic_util schema will onle have 1 record"""
        return f"""
        select
            proname,
            pronargs,
            proargtypes
        from
            pg_proc proc
        where
            pronamespace::regnamespace = '{self.schema}'::regnamespace
        """

    def get_compare_definition_query(self):
        """Only called in simulation. alembic_util schema will onle have 1 record"""
        return f"""
        select
            /*
                Body of the function (between $function$
                trim is required becasue functions may get rendered with
                inconsistent leading and trailing whitespace depending
            */
            trim(trim('\n', trim(trim('\t' from trim('\n' from trim(trim('\n' from trim(prosrc))))))))
            -- Other things that impact exact equality
            proname,
            procost,
            prorows,
            provariadic,
            prokind,
            proleakproof,
            proisstrict,
            proretset,
            provolatile,
            proparallel,
            pronargs,
            pronargdefaults,
            prorettype,
            proargtypes,
            proallargtypes,
            proargmodes,
            proargnames,
            proargdefaults
        from
            pg_proc proc
        where
            pronamespace::regnamespace = '{self.schema}'::regnamespace
        """
