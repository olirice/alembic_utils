import re
from collections.abc import Generator

from alembic_utils.exceptions import SQLParseFailure
from alembic_utils.replaceable_entity import ReplaceableEntity
from alembic_utils.statement import (
    coerce_to_unquoted,
    normalize_whitespace,
)
from sqlalchemy import text as sql_text
from sqlalchemy.sql.elements import TextClause

class PGRule(ReplaceableEntity):
    """A PostgreSQL Rule compatible with `alembic revision --autogenerate`.

    Args:
        ReplaceableEntity: Baseclass
        schema(str): schema the table lives in
        table(str): the table name the rule is associated with
        signature(str): the rules name
        definition(str): SQL definition of the rule

    """

    type_: str = "rule"

    def __init__(
        self,
        schema: str,
        table: str,
        signature: str,
        on_action: str,
        do_action: str,
        instead: bool = False,
    ):
        super().__init__(schema=schema, signature=signature, definition="")
        self.schema: str = coerce_to_unquoted(normalize_whitespace(schema))
        self.table: str = coerce_to_unquoted(normalize_whitespace(table))
        self.signature: str = coerce_to_unquoted(normalize_whitespace(signature))
        self.on_action: str = coerce_to_unquoted(normalize_whitespace(on_action))
        self.instead: str = "INSTEAD" if instead else ""
        self.do_action: str = coerce_to_unquoted(normalize_whitespace(do_action))
        self.on_entity: str = coerce_to_unquoted(f"{self.schema}.{self.table}")

    @classmethod
    def from_sql(cls, sql: str) -> "PGRule":
        """Create an Instance from a SQL string."""
        template = r"(?i)CREATE\s+RULE\s+(?P<signature>\S+)\s+AS\s+ON\s+(?P<on_action>\S+)\s+TO\s+(?P<on_entity>\S+)\s+DO\s+(?:(?P<instead>INSTEAD)\s+)?(?P<do_action>.+);"

        match = re.search(template, sql)

        if match:
            res = match.groupdict()
            signature = res["signature"]
            on_action = res["on_action"]
            on_entity = res["on_entity"]
            if "instead" in res:
                instead = True
            do_action = res["do_action"]

            schema, table = on_entity.split(".")

            return cls(schema, table, signature, on_action, do_action, instead)

        raise SQLParseFailure(f'Failed to parse SQL into PGRule """{sql}"""')

    def to_sql_statement_create(self) -> TextClause:
        """Generate a SQL "create rule" statement."""
        return sql_text(
            f'CREATE RULE "{self.signature}" AS ON {self.on_action} TO {self.on_entity} DO {self.instead} {self.do_action}'
        )

    def to_sql_statement_drop(self, cascade=False) -> TextClause:
        """Generate a SQL "drop rule" statement."""
        cascade = "cascade" if cascade else ""
        return sql_text(f'DROP RULE "{self.signature}" ON {self.on_entity} {cascade}')

    def to_sql_statement_create_or_replace(self) -> Generator[TextClause, None, None]:
        """Generate SQL equivalent to "create or replace" statement."""
        yield self.to_sql_statement_drop()
        yield self.to_sql_statement_create()

    @property
    def identity(self) -> str:
        """A string that consistently and globally identifies a rule."""
        # Rules identify by schema, table and name
        return f"{self.__class__.__name__}: {self.schema}.{self.table}.{self.signature}"

    def render_self_for_migration(self, omit_definition=False) -> str:
        """Render a string that is valid python code to reconstruct self in a migration."""
        var_name = self.to_variable_name()
        class_name = self.__class__.__name__
        escaped_definition = (
            self.definition if not omit_definition else "# not required for op"
        )

        return f"""{var_name} = {class_name}(
    schema = "{self.schema}",
    table = "{self.table}",
    signature = "{self.signature}",
    on_action = "{self.on_action}",
    do_action = "{self.do_action}",
    instead = {True if self.instead else False},
  )\n"""

    def to_variable_name(self) -> str:
        """A deterministic variable name based on PGRules's contents"""
        schema_name = self.schema.lower()
        table_name = self.table.lower()
        object_name = self.signature.split("(")[0].strip().lower().replace("-", "_")
        return f"{schema_name}_{table_name}_{object_name}"

    @classmethod
    def from_database(cls, sess, schema):
        """Get a list of all rules in a table."""
        sql = sql_text(f"""select definition from pg_rules where
            schemaname not in ('pg_catalog', 'information_schema')
            and schemaname::text like '{schema}';""")

        rows = sess.execute(sql).fetchall()
        db_rules = [cls.from_sql(x[0]) for x in rows]

        for rule in db_rules:
            assert rule is not None

        return db_rules
