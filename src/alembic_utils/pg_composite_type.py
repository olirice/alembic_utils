from typing import Generator

from parse import parse
from sqlalchemy import text as sql_text, TextClause

from alembic_utils.exceptions import SQLParseFailure
from alembic_utils.replaceable_entity import ReplaceableEntity
from alembic_utils.statement import strip_terminating_semicolon
from sqlalchemy.orm import Session


class PGCompositeType(ReplaceableEntity):
    """A PostgreSQL Composite Type compatible with `alembic revision --autogenerate`

    **Parameters:**

    * **schema** - *str*: A SQL schema name
    * **signature** - *str*: A SQL type's name
    * **definition** - *str*:  The type's definition including parenthesis (e.g. `(...)`)
    """

    type_ = "type"

    def __init__(self, schema: str, signature: str, definition: str):
        super().__init__(schema, signature, definition)
        self.definition: str = strip_terminating_semicolon(definition)

    @classmethod
    def from_sql(cls, sql: str) -> "PGCompositeType":
        """Create an instance from a SQL string"""
        template = "create{}type{:s}{schema}.{signature}{:s}as{:s}{definition}"
        result = parse(template, sql, case_sensitive=False)
        if result is not None:
            return cls(
                schema=result["schema"],
                # strip quote characters
                signature=result["signature"],
                definition=result["definition"],
            )

        raise SQLParseFailure(f'Failed to parse SQL into PGCompositeType """{sql}"""')

    def to_sql_statement_create(self) -> TextClause:
        """Generates a SQL "create type" statement"""
        return sql_text(
            f'CREATE TYPE {self.literal_schema}."{self.signature}" AS {self.definition};'
        )

    def to_sql_statement_drop(self, cascade=False) -> TextClause:
        """Generates a SQL "drop type" statement"""
        cascade = "cascade" if cascade else ""
        return sql_text(f'DROP TYPE {self.literal_schema}."{self.signature}" {cascade}')

    def to_sql_statement_create_or_replace(self) -> Generator[TextClause, None, None]:
        """Generates a SQL "create or replace type" statement"""
        yield sql_text(f'CREATE OR REPLACE TYPE {self.literal_schema}."{self.signature}" AS {self.definition};')

    @classmethod
    def from_database(cls, sess: Session, schema: str = "%"):
        """Get a list of all types defined in the db"""
        sql = sql_text(f"""
            SELECT
                nspname AS schema,
                pg_type.typname AS signature,
                string_agg(a.attname || ' ' || format_type(a.atttypid, a.atttypmod), ', ' ORDER BY a.attnum)
                    AS definition
            FROM pg_type
            LEFT JOIN pg_namespace ON pg_type.typnamespace = pg_namespace.oid
            LEFT JOIN pg_class ON pg_type.typrelid = pg_class.oid
            LEFT JOIN pg_attribute a ON a.attrelid = pg_type.typrelid
            WHERE
                typtype = 'c'
                AND (pg_class.relkind IS NULL OR pg_class.relkind <> 'r')
                AND nspname NOT IN ('pg_catalog', 'information_schema')
                and nspname::text like '{schema}'
            GROUP BY pg_type.typname, pg_namespace.nspname, pg_type.oid;
        """)
        rows = sess.execute(sql).fetchall()
        db_types = [cls(x[0], x[1], f"({x[2]})") for x in rows]

        for t in db_types:
            assert t is not None

        return db_types
