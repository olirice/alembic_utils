# pylint: disable=unused-argument,invalid-name,line-too-long
from __future__ import annotations

from typing import List

from parse import parse
from sqlalchemy import text as sql_text

from alembic_utils.exceptions import SQLParseFailure
from alembic_utils.replaceable_entity import ReplaceableEntity


class PGView(ReplaceableEntity):
    """A PostgreSQL View compatible with `alembic revision --autogenerate`

    **Parameters:**

    * **schema** - *str*: A SQL schema name
    * **signature** - *str*: A SQL view's call signature
    * **definition** - *str*: The SQL select statement body of the view
    """

    @classmethod
    def from_sql(cls, sql: str) -> PGView:
        """Create an instance from a SQL string"""
        template = "create{}view{:s}{schema}.{signature}{:s}as{:s}{definition}"
        result = parse(template, sql, case_sensitive=False)
        if result is not None:
            return cls(
                schema=result["schema"],
                # strip quote characters
                signature=result["signature"].replace('"', ""),
                definition=result["definition"],
            )

        raise SQLParseFailure(f'Failed to parse SQL into PGView """{sql}"""')

    def to_sql_statement_create(self) -> str:
        """Generates a SQL "create view" statement"""
        return sql_text(
            f'CREATE VIEW {self.literal_schema}."{self.signature}" AS {self.definition}'
        )

    def to_sql_statement_drop(self) -> str:
        """Generates a SQL "drop view" statement"""
        return sql_text(f'DROP VIEW {self.literal_schema}."{self.signature}"')

    def to_sql_statement_create_or_replace(self) -> str:
        """Generates a SQL "create or replace view" statement"""
        return sql_text(
            f'CREATE OR REPLACE VIEW {self.literal_schema}."{self.signature}" AS {self.definition}'
        )

    @classmethod
    def from_database(cls, connection, schema) -> List[PGView]:
        """Get a list of all functions defined in the db"""
        sql = sql_text(
            f"""
        select
            schemaname schema_name,
            viewname view_name,
            definition
        from
            pg_views
        where
            schemaname not in ('pg_catalog', 'information_schema')
            and schemaname::text = '{schema}';
        """
        )
        rows = connection.execute(sql).fetchall()
        db_views = [PGView(x[0], x[1], x[2]) for x in rows]

        for view in db_views:
            assert view is not None

        return db_views

    def get_compare_identity_query(self) -> str:
        """Return SQL string that returns 1 row for existing DB object"""
        return f"""
        select
            -- Schema is appended in python
            viewname view_name
        from
            pg_views
        where
            schemaname::text = '{self.schema}';
        """

    def get_compare_definition_query(self) -> str:
        """Return SQL string that returns 1 row for existing DB object"""
        return f"""
        select
            -- Schema is appended in python
            viewname view_name,
            definition
        from
	    pg_views
	where
            schemaname::text = '{self.schema}';
        """
