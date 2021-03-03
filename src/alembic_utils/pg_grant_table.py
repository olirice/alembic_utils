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


class Grant(str, Enum):
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    TRUNCATE = "TRUNCATE"
    REFERENCES = "REFERENCES"
    TRIGGER = "TRIGGER"

    def __str__(self) -> str:
        return str.__str__(self)

    def __repr__(self) -> str:
        return f"'{str.__str__(self)}'"


@dataclass(frozen=True, eq=True, order=True)
class SchemaTableRole:
    schema: str
    table: str
    role: str
    grant: Grant
    with_grant_option: str  # 'YES' or 'NO'


@dataclass
class PGGrantTable(ReplaceableEntity):
    """A PostgreSQL Grant Statement compatible with `alembic revision --autogenerate`

    PGGrantTable requires the role name being used to generate migrations to
    match the role name that executes migrations.

    If your system does not meet that requirement, disable them by excluding PGGrantTable
    from the `entity_types` list paramter of `alembic_utils.replaceable_entity.register_entities`

    **Parameters:**

    * **schema** - *str*: A SQL schema name
    * **table** - *str*: The table to grant access to
    * **columns** - *List[str]*: A list of column names on *table* to grant access to
    * **role** - *str*: The role to grant access to
    * **grant** - *Union[Grant, str]*: On of SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
    * **with_grant_option** - *bool*: Can the role grant access to other roles
    """

    schema: str
    table: str
    columns: List[str]
    role: str
    grant: Grant
    with_grant_option: bool

    type_ = "grant_table"

    def __init__(
        self,
        schema: str,
        table: str,
        columns: List[str],
        role: str,
        grant: Union[Grant, str],
        with_grant_option=False,
    ):
        self.schema: str = coerce_to_unquoted(schema)
        self.table: str = coerce_to_unquoted(table)
        self.columns: List[str] = sorted(columns)
        self.role: str = coerce_to_unquoted(role)
        self.grant: Grant = Grant(grant)
        self.with_grant_option: bool = with_grant_option
        self.signature = f"{self.schema}.{self.table}.{self.role}.{self.grant}"

    @classmethod
    def from_sql(cls, sql: str) -> "PGGrantTable":
        raise NotImplementedError()

    @property
    def identity(self) -> str:
        """A string that consistently and globally identifies a function"""
        # rows in information_schema.role_column_grants are uniquely identified by
        # the columns listed below + the grantor
        # be cautious when editing
        return f"{self.__class__.__name__}: {self.schema}.{self.table}.{self.role}.{self.grant}"

    @property
    def definition(self) -> str:  # type: ignore
        return str(self)

    def to_variable_name(self) -> str:
        """A deterministic variable name based on PGFunction's contents """
        schema_name = self.schema.lower()
        table_name = self.table.lower()
        role_name = self.role.lower()
        return f"{schema_name}_{table_name}_{role_name}_{str(self.grant)}".lower()

    def render_self_for_migration(self, omit_definition=False) -> str:
        """Render a string that is valid python code to reconstruct self in a migration"""
        var_name = self.to_variable_name()
        class_name = self.__class__.__name__

        return f"""{var_name} = {self}\n"""

    @classmethod
    def from_database(cls, sess: Session, schema: str = "%"):
        sql = sql_text(
            """
        SELECT
            table_schema as schema,
            table_name,
            grantee as role_name,
            privilege_type as grant_option,
            is_grantable,
            column_name
        FROM
            information_schema.role_column_grants rcg
            -- Cant revoke from superusers so filter out those recs
            join pg_roles pr
                on rcg.grantee = pr.rolname
        WHERE
            not pr.rolsuper
            and grantor = CURRENT_USER
            and table_schema like :schema
        """
        )

        rows = sess.execute(sql, params={"schema": schema}).fetchall()
        grants = []

        grouped = (
            flu(rows)
            .group_by(lambda x: SchemaTableRole(*x[:5]))
            .map(lambda x: (x[0], x[1].map_item(5).collect()))
            .collect()
        )
        for s_t_r, columns in grouped:
            grant = PGGrantTable(
                schema=s_t_r.schema,
                table=s_t_r.table,
                role=s_t_r.role,
                grant=s_t_r.grant,
                with_grant_option=s_t_r.with_grant_option == "YES",
                columns=columns,
            )
            grants.append(grant)
        return grants

    def to_sql_statement_create(self) -> TextClause:
        """Generates a SQL "create view" statement"""
        return sql_text(
            f'GRANT {self.grant} ( {", ".join(self.columns)} ) ON {self.literal_schema}.{coerce_to_quoted(self.table)} TO {coerce_to_quoted(self.role)}'
        )

    def to_sql_statement_drop(self, cascade=False) -> TextClause:
        """Generates a SQL "drop view" statement"""
        # cascade has no impact
        return sql_text(
            f"REVOKE {self.grant} ON {self.literal_schema}.{coerce_to_quoted(self.table)} FROM {coerce_to_quoted(self.role)}"
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
