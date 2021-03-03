# pylint: disable=unused-argument,invalid-name,line-too-long


from parse import parse
from sqlalchemy import text as sql_text
from sqlalchemy.sql.elements import TextClause

from alembic_utils.exceptions import SQLParseFailure
from alembic_utils.replaceable_entity import ReplaceableEntity
from alembic_utils.statement import strip_terminating_semicolon


class PGView(ReplaceableEntity):
    """A PostgreSQL View compatible with `alembic revision --autogenerate`

    **Parameters:**

    * **schema** - *str*: A SQL schema name
    * **signature** - *str*: A SQL view's call signature
    * **definition** - *str*: The SQL select statement body of the view
    """

    type_ = "view"

    @classmethod
    def from_sql(cls, sql: str) -> "PGView":
        """Create an instance from a SQL string"""
        template = "create{}view{:s}{schema}.{signature}{:s}as{:s}{definition}"
        result = parse(template, sql, case_sensitive=False)
        if result is not None:
            # If the signature includes column e.g. my_view (col1, col2, col3) remove them
            signature = result["signature"].split("(")[0]
            return cls(
                schema=result["schema"],
                # strip quote characters
                signature=signature.replace('"', ""),
                definition=strip_terminating_semicolon(result["definition"]),
            )

        raise SQLParseFailure(f'Failed to parse SQL into PGView """{sql}"""')

    def to_sql_statement_create(self) -> TextClause:
        """Generates a SQL "create view" statement"""
        return sql_text(
            f'CREATE VIEW {self.literal_schema}."{self.signature}" AS {self.definition};'
        )

    def to_sql_statement_drop(self, cascade=False) -> TextClause:
        """Generates a SQL "drop view" statement"""
        cascade = "cascade" if cascade else ""
        return sql_text(f'DROP VIEW {self.literal_schema}."{self.signature}" {cascade}')

    def to_sql_statement_create_or_replace(self) -> TextClause:
        """Generates a SQL "create or replace view" statement

        If the initial "CREATE OR REPLACE" statement does not succeed,
        fails over onto "DROP VIEW" followed by "CREATE VIEW"
        """
        return sql_text(
            f"""
        do $$
            begin
                CREATE OR REPLACE VIEW {self.literal_schema}."{self.signature}" AS {self.definition};

            exception when others then
                DROP VIEW IF EXISTS {self.literal_schema}."{self.signature}";

                CREATE VIEW {self.literal_schema}."{self.signature}" AS {self.definition};
            end;
        $$ language 'plpgsql'
        """
        )

    @classmethod
    def from_database(cls, sess, schema):
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
            and schemaname::text like '{schema}';
        """
        )
        rows = sess.execute(sql).fetchall()
        db_views = [PGView(x[0], x[1], x[2]) for x in rows]

        for view in db_views:
            assert view is not None

        return db_views
