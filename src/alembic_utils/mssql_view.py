# pylint: disable=unused-argument,invalid-name,line-too-long


from parse import parse
from sqlalchemy import text as sql_text
from sqlalchemy.sql.elements import TextClause

from alembic_utils.exceptions import SQLParseFailure
from alembic_utils.replaceable_entity import ReplaceableEntity


class MSSQLView(ReplaceableEntity):
    """A MSSQL View compatible with `alembic revision --autogenerate`

    **Parameters:**

    * **schema** - *str*: A SQL schema name
    * **signature** - *str*: A SQL view's call signature
    * **definition** - *str*: The SQL select statement body of the view
    """

    dialect = "mssql"
    type_ = "view"

    @classmethod
    def from_sql(cls, sql: str) -> "MSSQLView":
        """Create an instance from a SQL string"""
        template = "create{}view{:s}{schema}.{signature}{:s}as{:s}{definition}"
        result = parse(template, sql, case_sensitive=False)
        if result is not None:
            # If the signature includes column e.g. my_view (col1, col2, col3) remove them
            signature = result["signature"].split("(")[0]
            return cls(
                # strip bracket characters in schema and signature
                schema=result["schema"].replace("[", "").replace("]", ""),
                signature=signature.replace("[", "").replace("]", ""),
                definition=result["definition"],
            )

        raise SQLParseFailure(f'Failed to parse SQL into MSSQLView """{sql}"""')

    def to_sql_statement_create(self) -> TextClause:
        """Generates a SQL "create view" statement"""
        return sql_text(f"CREATE VIEW [{self.schema}].[{self.signature}] AS {self.definition}")

    def to_sql_statement_drop(self, cascade=False) -> TextClause:
        # TODO Should we handle cascade?
        """Generates a SQL "drop view" statement"""
        return sql_text(f"DROP VIEW [{self.schema}].[{self.signature}]")

    def to_sql_statement_create_or_replace(self) -> TextClause:
        """Generates a SQL "create or replace view" statement"""
        return sql_text(
            f"CREATE OR ALTER VIEW [{self.schema}].[{self.signature}] AS {self.definition}"
        )

    @classmethod
    def from_database(cls, connection, schema):
        """Get a list of all functions defined in the db"""
        sql = sql_text(
            f"""
            SELECT
                TABLE_SCHEMA schema_name,
                TABLE_NAME view_name,
				right(VIEW_DEFINITION, len(VIEW_DEFINITION) - charindex('AS', VIEW_DEFINITION)-2) definition
            FROM
                INFORMATION_SCHEMA.VIEWS
            WHERE
                TABLE_SCHEMA = '{schema}';
        """
        )
        rows = connection.execute(sql).fetchall()
        db_views = [MSSQLView(x.schema_name, x.view_name, x.definition) for x in rows]

        for view in db_views:
            assert view is not None

        return db_views
