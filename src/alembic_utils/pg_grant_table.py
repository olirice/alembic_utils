import re
from dataclasses import dataclass
from enum import Enum
from typing import Generator, List, Optional, Union

from flupy import flu
from sqlalchemy import text as sql_text
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import TextClause

from alembic_utils.exceptions import BadInputException
from alembic_utils.replaceable_entity import ReplaceableEntity
from alembic_utils.statement import escape_colon_for_sql


def _unquote_identifier(value: str) -> str:
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1].replace('""', '"')
    return value


class PGGrantTableChoice(str, Enum):
    # Applies at both table and column level
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    REFERENCES = "REFERENCES"
    # Applies only at table level
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
    role: Optional[str]
    grant: PGGrantTableChoice
    with_grant_option: str  # 'YES' or 'NO'


@dataclass
class PGGrantTable(ReplaceableEntity):
    """A PostgreSQL Grant Statement compatible with `alembic revision --autogenerate`

    PGGrantTable requires the role name being used to generate migrations to
    match the role name that executes migrations, and to act as the grantor for
    the privileges being managed. Reflection also reports grants by other grantors.

    If your system does not meet that requirement, disable them by excluding PGGrantTable
    in `include_object` https://alembic.sqlalchemy.org/en/latest/api/runtime.html#alembic.runtime.environment.EnvironmentContext.configure.params.include_object

    **Parameters:**

    * **schema** - *str*: A SQL schema name
    * **table** - *str*: The table to grant access to
    * **columns** - *Optional[List[str]]*: Column names, or None/[] for a table-level grant
    * **role** - *Optional[str]*: A named role, or None for the PUBLIC pseudo-role
    * **grant** - *Union[Grant, str]*: On of SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
    * **with_grant_option** - *bool*: Can the role grant access to other roles
    """

    schema: str
    table: str
    columns: List[str]
    role: Optional[str]
    grant: PGGrantTableChoice
    with_grant_option: bool

    type_ = "grant_table"

    def __init__(
        self,
        schema: str,
        table: str,
        role: Optional[str],
        grant: Union[PGGrantTableChoice, str],
        columns: Optional[List[str]] = None,
        with_grant_option=False,
    ):
        self.schema: str = _unquote_identifier(schema)
        self.table: str = _unquote_identifier(table)
        self.columns: List[str] = (
            sorted({_unquote_identifier(column) for column in columns}) if columns else []
        )
        # Strings retain their historical meaning as named roles, including PUBLIC.
        self.role: Optional[str] = _unquote_identifier(role) if role is not None else None
        self.grant: PGGrantTableChoice = PGGrantTableChoice(grant)
        self.with_grant_option: bool = with_grant_option
        self.signature = self.identity

        if self.columns and self.grant not in {C.SELECT, C.INSERT, C.UPDATE, C.REFERENCES}:
            raise BadInputException(
                f"When grant type is {self.grant} a value must not be provided for columns"
            )

    @classmethod
    def from_sql(cls, sql: str) -> "PGGrantTable":
        raise NotImplementedError()

    @property
    def identity(self) -> str:
        """A string that consistently and globally identifies a function"""
        # One entity manages a privilege for a grantee on a relation. Keep this
        # stable when columns change: replacement revokes before granting. Separate
        # create/drop identities would let a table REVOKE erase the new column grant.
        identity = f"{self.__class__.__name__}: {self.schema}.{self.table}.{self.role}.{self.grant}"
        # Keep named-role identities stable, without colliding with a role named None.
        return identity + " [PUBLIC]" if self.role is None else identity

    @property
    def definition(self) -> str:  # type: ignore
        return str(self)

    def to_variable_name(self) -> str:
        """A deterministic variable name based on PGFunction's contents"""
        role_name = self.role if self.role is not None else "public"
        name = re.sub(
            r"\W",
            "_",
            f"{self.schema}_{self.table}_{role_name}_{self.grant}".lower(),
            flags=re.ASCII,
        )
        return name if name.isidentifier() else "grant_" + name

    def render_self_for_migration(self, omit_definition=False) -> str:
        """Render a string that is valid python code to reconstruct self in a migration"""
        var_name = self.to_variable_name()
        class_name = self.__class__.__name__

        # Stored names are raw identifiers. Delimit them before reconstruction so
        # the constructor does not consume literal leading/trailing quote characters.
        quote = dialect().identifier_preparer.quote_identifier
        columns = [quote(column) for column in self.columns]
        role = quote(self.role) if self.role is not None else None
        return (
            f"{var_name} = {class_name}(schema={quote(self.schema)!r}, "
            f"table={quote(self.table)!r}, role={role!r}, grant={self.grant!r}, "
            f"columns={columns!r}, with_grant_option={self.with_grant_option!r})\n"
        )

    @classmethod
    def from_database(cls, sess: Session, schema: str = "%"):
        # relacl contains table grants; attacl contains only explicit column grants.
        # role_column_grants expands a table grant onto every existing column and
        # therefore loses the distinction (including access to future columns).
        sql = sql_text(
            """
            WITH grants AS (
                SELECT c.oid, c.relowner, n.nspname, c.relname,
                       acl.grantee, acl.privilege_type, acl.is_grantable,
                       NULL::text AS column_name
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) acl
                WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
                UNION ALL
                SELECT c.oid, c.relowner, n.nspname, c.relname,
                       acl.grantee, acl.privilege_type, acl.is_grantable,
                       a.attname::text AS column_name
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
                CROSS JOIN LATERAL pg_catalog.aclexplode(a.attacl) acl
                WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
                  AND a.attnum > 0 AND NOT a.attisdropped
            )
            SELECT g.nspname, g.relname,
                   CASE WHEN g.grantee = 0 THEN NULL
                        ELSE pr.rolname END AS role_name,
                   g.privilege_type,
                   CASE WHEN bool_or(g.is_grantable) THEN 'YES' ELSE 'NO' END,
                   g.column_name
            FROM grants g
            LEFT JOIN pg_catalog.pg_roles pr ON pr.oid = g.grantee
            WHERE g.nspname LIKE :schema
              AND g.nspname <> 'information_schema'
              AND g.nspname !~ '^pg_'
              AND g.grantee <> g.relowner
              AND NOT coalesce(pr.rolsuper, false)
              AND g.privilege_type IN
                  ('SELECT', 'INSERT', 'UPDATE', 'REFERENCES', 'DELETE', 'TRUNCATE', 'TRIGGER')
            GROUP BY g.nspname, g.relname, g.grantee, pr.rolname,
                     g.privilege_type, g.column_name
            ORDER BY g.nspname, g.relname, role_name, g.privilege_type, g.column_name
            """
        )
        rows = sess.execute(sql, params={"schema": schema}).fetchall()
        # Include scope in grouping, but not logical identity: simultaneous table
        # and column ACL entries must not become one accidental column-only grant.
        grouped = (
            flu(rows)
            .group_by(lambda x: (SchemaTableRole(*x[:5]), x[5] is None))
            .map(lambda x: (x[0], x[1].map_item(5).collect()))
            .collect()
        )
        return [
            cls(
                schema=dialect().identifier_preparer.quote_identifier(key.schema),
                table=dialect().identifier_preparer.quote_identifier(key.table),
                role=dialect().identifier_preparer.quote_identifier(key.role)
                if key.role is not None
                else None,
                grant=key.grant,
                with_grant_option=key.with_grant_option == "YES",
                columns=[]
                if table_level
                else [dialect().identifier_preparer.quote_identifier(column) for column in columns],
            )
            for (key, table_level), columns in grouped
        ]

    def get_database_definition(
        self, sess: Session, dependencies: Optional[List[ReplaceableEntity]] = None
    ) -> "PGGrantTable":
        # The existing entity identity owns the whole privilege, not an ACL item.
        # Do not silently discard a second scope or a different grant-option group
        # when producing a replacement and its downgrade.
        matches = [g for g in self.from_database(sess, self.schema) if g.identity == self.identity]
        if len(matches) > 1:
            raise BadInputException(
                f"Multiple ACL groups for {self.identity}; normalize the table/column and "
                "grant-option groups to one PGGrantTable declaration, or exclude this "
                "privilege with include_object and manage it in an explicit migration"
            )
        return super().get_database_definition(sess, dependencies)

    def to_sql_statement_create(self) -> TextClause:
        """Generate a table-level or column-level GRANT statement."""
        quote = dialect().identifier_preparer.quote_identifier
        with_grant_option = " WITH GRANT OPTION" if self.with_grant_option else ""
        columns = (
            f' ({", ".join(quote(column) for column in self.columns)})' if self.columns else ""
        )
        role = "PUBLIC" if self.role is None else quote(self.role)
        return sql_text(
            escape_colon_for_sql(
                f"GRANT {self.grant}{columns} ON {quote(self.schema)}.{quote(self.table)} "
                f"TO {role}{with_grant_option}"
            )
        )

    def to_sql_statement_drop(self, cascade=False) -> TextClause:
        """Revoke the entity's privilege, including its column grants.

        PostgreSQL can only revoke grants made by the acting grantor. To manage
        another grantor's ACL entries, generate and execute migrations as that role
        (for example with SET ROLE). Reflection does not confer that authority.
        """
        # One entity owns all columns for this schema/table/role/privilege identity.
        # Revoke the complete privilege so replacement also removes old columns.
        quote = dialect().identifier_preparer.quote_identifier
        role = "PUBLIC" if self.role is None else quote(self.role)
        return sql_text(
            escape_colon_for_sql(
                f"REVOKE {self.grant} ON {quote(self.schema)}.{quote(self.table)} FROM {role}"
            )
        )

    def to_sql_statement_create_or_replace(self) -> Generator[TextClause, None, None]:
        yield self.to_sql_statement_drop()
        yield self.to_sql_statement_create()
