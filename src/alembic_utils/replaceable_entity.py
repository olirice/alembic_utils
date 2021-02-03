# pylint: disable=unused-argument,invalid-name,line-too-long
import logging
from itertools import zip_longest
from pathlib import Path
from typing import List, Optional, Type, TypeVar

from alembic.autogenerate import comparators
from flupy import flu
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import TextClause

from alembic_utils.depends import solve_resolution_order
from alembic_utils.exceptions import (
    DuplicateRegistration,
    UnreachableException,
)
from alembic_utils.reversible_op import (
    CreateOp,
    DropOp,
    ReplaceOp,
    ReversibleOp,
)
from alembic_utils.simulate import simulate_entity
from alembic_utils.statement import (
    coerce_to_unquoted,
    escape_colon,
    normalize_whitespace,
    strip_terminating_semicolon,
)

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="ReplaceableEntity")


class ReplaceableEntity:
    """A SQL Entity that can be replaced"""

    def __init__(self, schema: str, signature: str, definition: str):
        self.schema: str = coerce_to_unquoted(normalize_whitespace(schema))
        self.signature: str = coerce_to_unquoted(normalize_whitespace(signature))
        self.definition: str = escape_colon(strip_terminating_semicolon(definition))

    @classmethod
    def from_sql(cls: Type[T], sql: str) -> T:
        """Create an instance from a SQL string"""
        raise NotImplementedError()

    @property
    def literal_schema(self) -> str:
        """Wrap a schema name in literal quotes
        Useful for emitting SQL statements
        """
        return f'"{self.schema}"'

    @classmethod
    def from_path(cls: Type[T], path: Path) -> T:
        """Create an instance instance from a SQL file path"""
        with path.open() as sql_file:
            sql = sql_file.read()
        return cls.from_sql(sql)

    @classmethod
    def from_database(cls, sess: Session, schema="%") -> List[T]:
        """Collect existing entities from the database for given schema"""
        raise NotImplementedError()

    def to_sql_statement_create(self) -> TextClause:
        """ Generates a SQL "create function" statement for PGFunction """
        raise NotImplementedError()

    def to_sql_statement_drop(self, cascade=False) -> TextClause:
        """ Generates a SQL "drop function" statement for PGFunction """
        raise NotImplementedError()

    def to_sql_statement_create_or_replace(self) -> TextClause:
        """ Generates a SQL "create or replace function" statement for PGFunction """
        raise NotImplementedError()

    def get_database_definition(
        self: T, sess: Session, dependencies: Optional[List["ReplaceableEntity"]] = None
    ) -> T:  # $Optional[T]:
        """Creates the entity in the database, retrieves its 'rendered' then rolls it back"""
        with simulate_entity(sess, self, dependencies) as sess:
            # Drop self
            sess.execute(self.to_sql_statement_drop())

            # collect all remaining entities
            db_entities: List[T] = sorted(
                self.from_database(sess, schema=self.schema), key=lambda x: x.identity
            )

        with simulate_entity(sess, self, dependencies) as sess:
            # collect all remaining entities
            all_w_self: List[T] = sorted(
                self.from_database(sess, schema=self.schema), key=lambda x: x.identity
            )

        # Find "self" by diffing the before and after
        for without_self, with_self in zip_longest(db_entities, all_w_self):
            if without_self is None or without_self.identity != with_self.identity:
                return with_self

        raise UnreachableException()

    def render_self_for_migration(self, omit_definition=False) -> str:
        """Render a string that is valid python code to reconstruct self in a migration"""
        var_name = self.to_variable_name()
        class_name = self.__class__.__name__
        escaped_definition = self.definition if not omit_definition else "# not required for op"

        return f"""{var_name} = {class_name}(
    schema="{self.schema}",
    signature="{self.signature}",
    definition={repr(escaped_definition)}
)\n"""

    @classmethod
    def render_import_statement(cls) -> str:
        """Render a string that is valid python code to import current class"""
        module_path = cls.__module__
        class_name = cls.__name__
        return f"from {module_path} import {class_name}\nfrom sqlalchemy import text as sql_text"

    @property
    def identity(self) -> str:
        """A string that consistently and globally identifies a function"""
        return f"{self.__class__.__name__}: {self.schema}.{self.signature}"

    def to_variable_name(self) -> str:
        """A deterministic variable name based on PGFunction's contents """
        schema_name = self.schema.lower()
        object_name = self.signature.split("(")[0].strip().lower()
        return f"{schema_name}_{object_name}"

    def get_required_migration_op(
        self: T, sess: Session, dependencies: Optional[List["ReplaceableEntity"]] = None
    ) -> Optional[ReversibleOp]:
        """Get the migration operation required for autogenerate"""
        # All entities in the database for self's schema
        entities_in_database: List[T] = self.from_database(sess, schema=self.schema)

        db_def = self.get_database_definition(sess, dependencies=dependencies)

        for x in entities_in_database:

            if (db_def.identity, normalize_whitespace(db_def.definition)) == (
                x.identity,
                normalize_whitespace(x.definition),
            ):
                return None

            if db_def.identity == x.identity:
                return ReplaceOp(self)

        return CreateOp(self)


##################
# Event Listener #
##################


def register_entities(
    entities: List[T],
    schemas: Optional[List[str]] = None,
    exclude_schemas: Optional[List[str]] = None,
) -> None:
    """Create an event listener to watch for changes in registered entities when migrations are created using
    `alembic revision --autogenerate`

    **Parameters:**

    * **entities** - *List[ReplaceableEntity]*: A list of entities (PGFunction, PGView, etc) to monitor for revisions
    * **schemas** - *Optional[List[str]]*: A list of SQL schema names to monitor. Note, schemas referenced in registered entities are automatically monitored.
    * **exclude_schemas** - *Optional[List[str]]*: A list of SQL schemas to ignore. Note, explicitly registered entities will still be monitored.
    """

    @comparators.dispatch_for("schema")
    def compare_registered_entities(
        autogen_context, upgrade_ops, sqla_schemas: Optional[List[Optional[str]]]
    ):
        connection = autogen_context.connection

        # Ensure pg_functions have unique identities (not registered twice)
        for ident, function_group in flu(entities).group_by(key=lambda x: x.identity):
            if len(function_group.collect()) > 1:
                raise DuplicateRegistration(
                    f"PGFunction with identity {ident} was registered multiple times"
                )

        all_schema_references: List[str] = []

        # User registered schemas + automatically registered schemas (from SQLA Metadata)
        if schemas is not None:
            for schema in schemas:
                if schema is not None:
                    all_schema_references.append(schema)

        if sqla_schemas is not None:
            to_add = [x for x in sqla_schemas if x]
            all_schema_references.extend(to_add)

        for entity in entities:
            all_schema_references.append(entity.schema)

        # Remove excluded schemas
        observed_schemas = [
            x for x in set(all_schema_references) if x not in (exclude_schemas or [])
        ]

        # Solve resolution order
        try:
            transaction = connection.begin()
            sess = Session(bind=connection)
            ordered_entities: List[T] = solve_resolution_order(sess, entities)
        finally:
            transaction.rollback()

        # entities that are receiving a create or update op
        has_create_or_update_op: List[ReplaceableEntity] = []

        # database rendered definitions for the entities we have a local instance for
        # Note: used for drops
        local_entities = []

        # Required migration OPs, Create/Update/NoOp
        for entity in ordered_entities:
            logger.info(
                "Detecting required migration op %s %s",
                entity.__class__.__name__,
                entity.identity,
            )

            try:
                transaction = connection.begin()
                sess = Session(bind=connection)

                maybe_op = entity.get_required_migration_op(
                    sess, dependencies=has_create_or_update_op
                )

                local_db_def = entity.get_database_definition(
                    sess, dependencies=has_create_or_update_op
                )
                local_entities.append(local_db_def)

                if maybe_op:
                    upgrade_ops.ops.append(maybe_op)
                    has_create_or_update_op.append(entity)

                    logger.info(
                        "Detected %s op for %s %s",
                        maybe_op.__class__.__name__,
                        entity.__class__.__name__,
                        entity.identity,
                    )
                else:
                    logger.info(
                        "Detected NoOp op for %s %s",
                        entity.__class__.__name__,
                        entity.identity,
                    )

            finally:
                transaction.rollback()

        # Required migration OPs, Drop
        try:
            # Start a parent transaction
            # Bind the session within the parent transaction
            transaction = connection.begin()
            sess = Session(bind=connection)

            # All database entities currently live
            # Check if anything needs to drop
            for entity_class in ReplaceableEntity.__subclasses__():

                # Entities within the schemas that are live
                for schema in observed_schemas:

                    db_entities: List[ReplaceableEntity] = entity_class.from_database(
                        sess, schema=schema
                    )

                    # Check for functions that were deleted locally
                    for db_entity in db_entities:
                        for local_entity in local_entities:
                            if db_entity.identity == local_entity.identity:
                                break
                        else:
                            # No match was found locally
                            upgrade_ops.ops.append(DropOp(db_entity))
                            logger.info(
                                "Detected DropOp op for %s %s",
                                db_entity.__class__.__name__,
                                db_entity.identity,
                            )

        finally:
            transaction.rollback()
