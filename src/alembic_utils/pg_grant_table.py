from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Union

from flupy import flu
from sqlalchemy import text as sql_text
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import TextClause

from alembic_utils.exceptions import BadInputException
from alembic_utils.replaceable_entity import ReplaceableEntity
from alembic_utils.statement import coerce_to_quoted, coerce_to_unquoted


class PGGrantTableChoice(str, Enum):
    # Applies at column level
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    REFERENCES = "REFERENCES"
    # Applies at table Level
    DELETE = "DELETE"
    TRUNCATE = "TRUNCATE"
    TRIGGER = "TRIGGER"

    def __str__(self) -> str:
        return str.__str__(self)

    def __repr__(self) -> str:
        return f"'{str.__str__(self)}'"


C = PGGrantTableChoice


@dataclass(frozen=True, eq=True, order=True)
class SchemaTableRole:
    schema: str
    table: str
    role: str
    grant: PGGrantTableChoice
    with_grant_option: str  # 'YES' or 'NO'


@dataclass
class PGGrantTable(ReplaceableEntity):
    """A PostgreSQL Grant Statement compatible with `alembic revision --autogenerate`

    PGGrantTable requires the role name being used to generate migrations to
    match the role name that executes migrations.

    If your system does not meet that requirement, disable them by excluding PGGrantTable
    in `include_object` https://alembic.sqlalchemy.org/en/latest/api/runtime.html#alembic.runtime.environment.EnvironmentContext.configure.params.include_object

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
    grant: PGGrantTableChoice
    with_grant_option: bool

    type_ = "grant_table"

    def __init__(
        self,
        schema: str,
        table: str,
        role: str,
        grant: Union[PGGrantTableChoice, str],
        columns: Optional[List[str]] = None,
        with_grant_option=False,
    ):
        self.schema: str = coerce_to_unquoted(schema)
        self.table: str = coerce_to_unquoted(table)
        self.columns: List[str] = sorted(columns) if columns else []
        self.role: str = coerce_to_unquoted(role)
        self.grant: PGGrantTableChoice = PGGrantTableChoice(grant)
        self.with_grant_option: bool = with_grant_option
        self.signature = self.identity

        if PGGrantTableChoice(self.grant) in {C.SELECT, C.INSERT, C.UPDATE, C.REFERENCES}:
            if len(self.columns) == 0:
                raise BadInputException(
                    f"When grant type is {self.grant} a value must be provided for columns"
                )
        else:
            if self.columns:
                raise BadInputException(
                    f"When grant type is {self.grant} a value must not be provided for columns"
                )

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
        # COLUMN LEVEL
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
            and privilege_type in ('SELECT', 'INSERT', 'UPDATE', 'REFERENCES')
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

        # TABLE LEVEL
        sql = sql_text(
            """
        SELECT
            table_schema as schema_name,
            table_name,
            grantee as role_name,
            privilege_type as grant_option,
            is_grantable
        FROM
            information_schema.role_table_grants rcg
            -- Cant revoke from superusers so filter out those recs
            join pg_roles pr
                on rcg.grantee = pr.rolname
        WHERE
            not pr.rolsuper
            and grantor = CURRENT_USER
            and table_schema like :schema
            and privilege_type in ('DELETE', 'TRUNCATE', 'TRIGGER')
        """
        )

        rows = sess.execute(sql, params={"schema": schema}).fetchall()

        for schema_name, table_name, role_name, grant_option, is_grantable in rows:
            grant = PGGrantTable(
                schema=schema_name,
                table=table_name,
                role=role_name,
                grant=grant_option,
                with_grant_option=is_grantable == "YES",
            )
            grants.append(grant)
        return grants

    def to_sql_statement_create(self) -> TextClause:
        """Generates a SQL "create view" statement"""
        with_grant_option = " WITH GRANT OPTION" if self.with_grant_option else ""
        maybe_columns_clause = f'( {", ".join(self.columns)} )' if self.columns else ""
        return sql_text(
            f"GRANT {self.grant} {maybe_columns_clause} ON {self.literal_schema}.{coerce_to_quoted(self.table)} TO {coerce_to_quoted(self.role)} {with_grant_option}"
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
                {self.to_sql_statement_create()};
            end;
        $$ language 'plpgsql'
        """
        )
