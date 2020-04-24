# pylint: disable=unused-argument,invalid-name,line-too-long
from __future__ import annotations

from typing import List, Optional, Set

from parse import parse
from sqlalchemy import text as sql_text

from alembic_utils.exceptions import SQLParseFailure
from alembic_utils.replaceable_entity import ReplaceableEntity


class PGFunction(ReplaceableEntity):
    """ A PostgreSQL Function that can be versioned and replaced """

    @classmethod
    def from_sql(cls, sql: str) -> Optional[PGFunction]:
        """ Create an instance of PGFunction from a blob of sql """
        template = (
            "create{:s}or{:s}replace{:s}function{:s}{schema}.{signature}{:s}returns{:s}{definition}"
        )
        result = parse(template, sql.strip(), case_sensitive=False)
        if result is not None:
            return cls(
                schema=result["schema"],
                signature=result["signature"],
                definition="returns " + result["definition"],
            )
        return None

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

    def to_sql_statement_create(self) -> str:
        """ Generates a SQL "create function" statement for PGFunction """
        return f"CREATE FUNCTION {self.schema}.{self.signature} {self.definition}"

    def to_sql_statement_drop(self) -> str:
        """ Generates a SQL "drop function" statement for PGFunction """
        return f"DROP FUNCTION {self.schema}.{self.signature}"

    def to_sql_statement_create_or_replace(self) -> str:
        """ Generates a SQL "create or replace function" statement for PGFunction """
        return f"CREATE OR REPLACE FUNCTION {self.schema}.{self.signature} {self.definition}"

    def get_definition_body(self) -> str:
        templates = ["{}$${body}$${}", "{}${}${body}${}$", "{}${}${body}${}${}"]
        for template in templates:
            result = parse(template, self.definition, case_sensitive=False)
            if result is not None:
                return result["body"]
        raise SQLParseFailure(self.definition)

    def get_definition_qualifiers(self) -> Set[str]:
        all_qualifiers = [
            "window",
            "immutable",
            "stable",
            "volatile",
            "called on null input",
            "returns null on null input",
            "strict",
            "external security invoker",
            "security invoker",
            "external security definer",
            "security definer",
        ]

        lower_def = self.definition.lower()

        found_qualifiers = set()
        for qualifier in all_qualifiers:
            if qualifier in lower_def:
                found_qualifiers.add(qualifier)
        return found_qualifiers

    def is_equal_definition(self, other: PGFunction, connection) -> bool:
        """ Is the definition within self and other the same """
        self_body = self.get_definition_body().strip()
        other_body = other.get_definition_body().strip()
        self_qualifiers = self.get_definition_qualifiers()
        other_qualifiers = other.get_definition_qualifiers()
        return self_body == other_body and self_qualifiers == other_qualifiers
