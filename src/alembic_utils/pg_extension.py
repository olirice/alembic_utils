# pylint: disable=unused-argument,invalid-name,line-too-long


from sqlalchemy import text as sql_text
from sqlalchemy.sql.elements import TextClause

from alembic_utils.replaceable_entity import ReplaceableEntity
from alembic_utils.statement import (
    coerce_to_quoted,
    coerce_to_unquoted,
    escape_colon_for_sql,
    normalize_whitespace,
    strip_terminating_semicolon,
)


class PGExtension(ReplaceableEntity):
    """A PostgreSQL Extension compatible with `alembic revision --autogenerate`

    **Parameters:**

    * **schema** - *str*: A SQL schema name
    * **signature** - *str*: A PostgreSQL extension's name
    """

    type_ = "extension"

    def __init__(self, schema: str, signature: str):
        self.schema: str = coerce_to_unquoted(normalize_whitespace(schema))
        self.signature: str = coerce_to_unquoted(normalize_whitespace(signature))
        # Include schema in definition since extensions can only exist once per
        # database and we want to detect schema changes and emit alter schema
        self.definition: str = f"{self.__class__.__name__}: {self.schema} {self.signature}"

    def to_sql_statement_create(self) -> TextClause:
        """Generates a SQL "create extension" statement"""
        return sql_text(f'CREATE EXTENSION "{self.signature}" WITH SCHEMA {self.literal_schema};')

    def to_sql_statement_drop(self, cascade=False) -> TextClause:
        """Generates a SQL "drop extension" statement"""
        cascade = "CASCADE" if cascade else ""
        return sql_text(f'DROP EXTENSION "{self.signature}" {cascade}')

    def to_sql_statement_create_or_replace(self) -> TextClause:
        """Generates SQL equivalent to "create or replace" statement"""
        raise NotImplementedError()

    @property
    def identity(self) -> str:
        """A string that consistently and globally identifies an extension"""
        # Extensions may only be installed once per db, schema is not a
        # component of identity
        return f"{self.__class__.__name__}: {self.signature}"

    def render_self_for_migration(self, omit_definition=False) -> str:
        """Render a string that is valid python code to reconstruct self in a migration"""
        var_name = self.to_variable_name()
        class_name = self.__class__.__name__

        return f"""{var_name} = {class_name}(
    schema="{self.schema}",
    signature="{self.signature}"
)\n"""

    @classmethod
    def from_database(cls, sess, schema):
        """Get a list of all extensions defined in the db"""
        sql = sql_text(
            f"""
        select
            np.nspname schema_name,
            ext.extname extension_name
        from
            pg_extension ext
            join pg_namespace np
                on ext.extnamespace = np.oid
        where
            np.nspname not in ('pg_catalog')
            and np.nspname like :schema;
        """
        )
        rows = sess.execute(sql, {"schema": schema}).fetchall()
        db_exts = [PGExtension(x[0], x[1]) for x in rows]
        return db_exts
