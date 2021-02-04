from dataclasses import dataclass
from enum import Enum
from typing import List, Union

from flupy import flu
from sqlalchemy import text as sql_text
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import TextClause

from alembic_utils.replaceable_entity import ReplaceableEntity
from alembic_utils.statement import coerce_to_quoted, coerce_to_unquoted

# GRANT EXECUTE, ALL PRIVILEGES ON FUNCTION / ALL FUNCTIONS IN SCHEMA


class GrantOption(str, Enum):
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    TRUNCATE = "TRUNCATE"
    REFERENCES = "REFERENCES"
    TRIGGER = "TRIGGER"
    ALL = "ALL"

    def __str__(self) -> str:
        return str.__str__(self)

    def __repr__(self) -> str:
        return str.__str__(self)


@dataclass(frozen=True, eq=True, order=True)
class SchemaTableRole:
    schema: str
    table: str
    role: str


@dataclass
class PGGrantTable(ReplaceableEntity):

    schema: str
    table: str
    role: str
    grant_options: List[GrantOption]

    def __init__(
        self, schema: str, table: str, role: str, grant_options: List[Union[GrantOption, str]]
    ):
        self.schema: str = coerce_to_unquoted(schema)
        self.table: str = coerce_to_unquoted(table)
        self.role: str = coerce_to_unquoted(role)
        self.grant_options: List[GrantOption] = sorted([GrantOption(x) for x in grant_options])

    @classmethod
    def from_sql(cls, sql: str) -> "PGGrantTable":
        raise NotImplementedError()

    @property
    def identity(self) -> str:
        """A string that consistently and globally identifies a function"""
        return f"{self.__class__.__name__}: {self.schema}.{self.table}.{self.role}"

    @property
    def definition(self) -> str:  # type: ignore
        return f"{self.__class__.__name__}: {self.schema}.{self.table}.{self.role} {' '.join([str(x) for x in sorted(self.grant_options)])}"

    def to_variable_name(self) -> str:
        """A deterministic variable name based on PGFunction's contents """
        schema_name = self.schema.lower()
        table_name = self.table.lower()
        role_name = self.role.lower()
        return f"{schema_name}_{table_name}_{role_name}"

    def render_self_for_migration(self, omit_definition=False) -> str:
        """Render a string that is valid python code to reconstruct self in a migration"""
        var_name = self.to_variable_name()
        class_name = self.__class__.__name__

        return f"""{var_name} = {class_name}(
    schema="{self.schema}",
    table="{self.table}",
    role="{self.role}",
    grant_options={[str(x) for x in self.grant_options]},
)\n"""

    @classmethod
    def from_database(cls, sess: Session, schema: str = "%"):
        sql = sql_text(
            """
        SELECT table_schema, table_name, grantee as role_name, privilege_type as grant_option
        FROM information_schema.role_table_grants
        WHERE table_schema like :schema
        """
        )

        rows = sess.execute(sql, params={"schema": schema}).fetchall()
        grants = []

        grouped = (
            flu(rows)
            .group_by(lambda x: SchemaTableRole(*x[:3]))
            .map(lambda x: (x[0], x[1].map_item(3).collect()))
            .collect()
        )
        for s_t_r, grant_options in grouped:
            grant = PGGrantTable(
                schema=s_t_r.schema,
                table=s_t_r.table,
                role=s_t_r.role,
                grant_options=grant_options,
            )
            grants.append(grant)
        return grants

    def to_sql_statement_create(self) -> TextClause:
        """Generates a SQL "create view" statement"""
        return sql_text(
            f'GRANT {", ".join([str(x) for x in self.grant_options])} ON TABLE {self.literal_schema}.{coerce_to_quoted(self.table)} TO {coerce_to_quoted(self.role)}'
        )

    def to_sql_statement_drop(self, cascade=False) -> TextClause:
        """Generates a SQL "drop view" statement"""
        # cascade has no impact
        return sql_text(
            f"REVOKE ALL ON TABLE {self.literal_schema}.{coerce_to_quoted(self.table)} FROM {coerce_to_quoted(self.role)}"
        )

    def to_sql_statement_create_or_replace(self) -> TextClause:
        return sql_text(
            f"""
        do $$
            begin
                {self.to_sql_statement_drop()};

            exception when others then
                {self.to_sql_statement_create()};
                {self.to_sql_statement_drop()};
            end;
        $$ language 'plpgsql'
        """
        )
