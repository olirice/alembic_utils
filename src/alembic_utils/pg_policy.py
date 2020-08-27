# pylint: disable=unused-argument,invalid-name,line-too-long
from __future__ import annotations

from typing import List

from parse import parse
from sqlalchemy import text as sql_text

from alembic_utils.exceptions import SQLParseFailure
from alembic_utils.replaceable_entity import ReplaceableEntity


class PGPolicy(ReplaceableEntity):
    """A PostgreSQL Function compatible with `alembic revision --autogenerate`

    **Parameters:**

    * **schema** - *str*: A SQL schema name
    * **signature** - *str*: A SQL policy name and tablename, separated by "."
    * **definition** - *str*:  The definition of the policy, incl. permissive, for, to, using, with check

    """

    is_replaceable = False

    @classmethod
    def from_sql(cls, sql: str) -> PGPolicy:
        """Create an instance instance from a SQL string"""

        template = "create policy{signature}on{schema}.{tablename}{:s}{definition}"
        result = parse(template, sql.strip(), case_sensitive=False)

        if result is not None:
            return cls(
                schema=result["schema"],
                signature=f'{result["signature"]}.{result["tablename"]}',
                definition=result["definition"],
            )
        raise SQLParseFailure(f'Failed to parse SQL into PGPolicy """{sql}"""')

    @property
    def policyname(self):
        return self.signature.split(".")[0]

    @property
    def tablename(self):
        return self.signature.split(".")[1]

    def to_sql_statement_create(self) -> str:
        """ Generates a SQL "create poicy" statement for PGPolicy """

        return sql_text(
            f"CREATE POLICY {self.policyname} on {self.schema}.{self.tablename} {self.definition}"
        )

    def to_sql_statement_drop(self) -> str:
        """Generates a SQL "drop policy" statement for PGPolicy"""

        return sql_text(f"DROP POLICY {self.policyname} on {self.schema}.{self.tablename}")

    def to_sql_statement_create_or_replace(self) -> str:
        """Not implemented, postgres policies do not support replace."""
        raise NotImplementedError()

    @classmethod
    def from_database(cls, connection, schema) -> List[PGPolicy]:
        """Get a list of all policies defined in the db"""
        sql = sql_text(
            f"""
        select
            schemaname,
            tablename,
            policyname,
            permissive,
            roles,
            cmd,
            qual,
            with_check
        from
            pg_policies
        where
            schemaname = '{schema}'
        """
        )
        rows = connection.execute(sql).fetchall()

        def get_definition(permissive, cmd, roles, qual, with_check):
            definition = ""
            if permissive is not None:
                definition += f"as {permissive}"
            if cmd is not None:
                definition += f"for {cmd}"
            if roles is not None:
                definition += f"to {','.join(roles)}"
            if qual is not None:
                definition += f"using {qual}"
            if with_check is not None:
                definition += f"with check {with_check}"

        db_policies = [PGPolicy(x[0], f"{x[2]}.{x[1]}", get_definition(*x[3:])) for x in rows]

        for policy in db_policies:
            assert policy is not None

        return db_policies

    def get_compare_identity_query(self):
        """Only called in simulation. alembic_util schema will only have 1 record"""
        return f"""
        select
            tablename,
            policyname
        from
            pg_policies
        where
            schemaname = '{self.schema}'
        """

    def get_compare_definition_query(self):
        """Only called in simulation. alembic_util schema will only have 1 record"""

        return f"""
        select
            permissive,
            roles,
            cmd,
            qual,
            with_check
        from
            pg_policies
        where
            schemaname = '{self.schema}'
        """
