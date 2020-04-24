# pylint: disable=unused-argument,invalid-name,line-too-long
from __future__ import annotations

from typing import List, Optional

from parse import parse
from sqlalchemy import text as sql_text

from alembic_utils.exceptions import SQLParseFailure
from alembic_utils.replaceable_entity import ReplaceableEntity


class PGView(ReplaceableEntity):
    """ A PostgreSQL View that can be versioned and replaced """

    @classmethod
    def from_sql(cls, sql: str) -> Optional[PGView]:
        """Create an instance of PGFunction from a blob of sql"""
        templates = [
            "create{:s}or{:s}replace{:s}view{:s}{schema}.{signature}{:s}as{:s}{definition}",
            "",
        ]
        for template in templates:
            result = parse(template, sql, case_sensitive=False)
            if result is not None:
                return cls(
                    schema=result["schema"],
                    signature=result["signature"],
                    definition=result["definition"],
                )

        raise SQLParseFailure(sql)

    @classmethod
    def from_database(cls, connection, schema="%") -> List[PGView]:
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
            and schemaname like '{schema}';
        """
        )
        rows = connection.execute(sql).fetchall()
        db_views = [PGView(x[0], x[1], x[2]) for x in rows]

        for view in db_views:
            assert view is not None

        return db_views

    def to_sql_statement_create(self) -> str:
        """Generates a SQL "create view" statement"""
        return f"CREATE VIEW {self.schema}.{self.signature} AS {self.definition}"

    def to_sql_statement_drop(self) -> str:
        """Generates a SQL "drop view" statement"""
        return f"DROP VIEW {self.schema}.{self.signature}"

    def to_sql_statement_create_or_replace(self) -> str:
        """Generates a SQL "create or replace view" statement"""
        return f"CREATE OR REPLACE VIEW {self.schema}.{self.signature} AS {self.definition}"
