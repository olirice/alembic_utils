# pylint: disable=unused-argument,invalid-name,line-too-long
import logging
from itertools import zip_longest
from pathlib import Path
from typing import List, Optional, Set, Type, TypeVar

from alembic.autogenerate import comparators
from alembic.autogenerate.api import AutogenContext
from flupy import flu
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import TextClause

import alembic_utils
from alembic_utils.depends import solve_resolution_order
from alembic_utils.exceptions import (
    DuplicateRegistration,
    UnreachableException,
)
from alembic_utils.experimental import collect_subclasses
from alembic_utils.reversible_op import (
    CreateOp,
    DropOp,
    ReplaceOp,
    ReversibleOp,
)
from alembic_utils.simulate import simulate_entity
from alembic_utils.statement import (
    coerce_to_quoted,
    coerce_to_unquoted,
    escape_colon_for_sql,
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
        self.definition: str = escape_colon_for_sql(strip_terminating_semicolon(definition))

    @property
    def type_(self) -> str:
        """In order to support calls to `run_name_filters` and
        `run_object_filters` on the AutogenContext object, each
        entity needs to have a named type.
        https://alembic.sqlalchemy.org/en/latest/api/autogenerate.html#alembic.autogenerate.api.AutogenContext.run_name_filters
        """
        raise NotImplementedError()

    @classmethod
    def from_sql(cls: Type[T], sql: str) -> T:
        """Create an instance from a SQL string"""
        raise NotImplementedError()

    @property
    def literal_schema(self) -> str:
        """Wrap a schema name in literal quotes
        Useful for emitting SQL statements
        """
        return coerce_to_quoted(self.schema)

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
    entity_types: Optional[List[Type[ReplaceableEntity]]] = None,
) -> None:
    """Create an event listener to watch for changes in registered entities when migrations are created using
    `alembic revision --autogenerate`

    **Parameters:**

    * **entities** - *List[ReplaceableEntity]*: A list of entities (PGFunction, PGView, etc) to monitor for revisions

    **Deprecated Parameters:**

    .. deprecated:: 0.5.1 for removal in 0.6.0

    *Configure schema and object inclusion/exclusion with `include_name` and `include_object` in `env.py`. For more information see https://alembic.sqlalchemy.org/en/latest/autogenerate.html#controlling-what-to-be-autogenerated*


    * **schemas** - *Optional[List[str]]*: A list of SQL schema names to monitor. Note, schemas referenced in registered entities are automatically monitored.
    * **exclude_schemas** - *Optional[List[str]]*: A list of SQL schemas to ignore. Note, explicitly registered entities will still be monitored.
    * **entity_types** - *Optional[List[str]]*: A list of ReplaceableEntity classes to consider during migrations. Other entity types are ignored
    """

    allowed_entity_types: List[Type[ReplaceableEntity]] = entity_types or collect_subclasses(
        alembic_utils, ReplaceableEntity
    )

    @comparators.dispatch_for("schema")
    def compare_registered_entities(
        autogen_context: AutogenContext,
        upgrade_ops,
        sqla_schemas: Optional[List[Optional[str]]],
    ):
        connection = autogen_context.connection

        # Ensure pg_functions have unique identities (not registered twice)
        for ident, function_group in flu(entities).group_by(key=lambda x: x.identity):
            if len(function_group.collect()) > 1:
                raise DuplicateRegistration(
                    f"PGFunction with identity {ident} was registered multiple times"
                )

        # https://alembic.sqlalchemy.org/en/latest/autogenerate.html#controlling-what-to-be-autogenerated
        # if EnvironmentContext.configure.include_schemas is True, all non-default "scehmas" should be included
        # pulled from the inspector
        include_schemas: bool = autogen_context.opts["include_schemas"]

        reflected_schemas = set(
            autogen_context.inspector.get_schema_names() if include_schemas else []  # type: ignore
        )
        sqla_schemas: Set[Optional[str]] = sqla_schemas or {}  # type: ignore
        manual_schemas = set(schemas or [])  # Deprecated for remove in 0.6.0
        entity_schemas = {x.schema for x in entities}  # from ReplaceableEntity instances
        all_schema_references = reflected_schemas | sqla_schemas | manual_schemas | entity_schemas  # type: ignore

        # Remove excluded schemas
        observed_schemas: Set[str] = {
            schema_name
            for schema_name in all_schema_references
            if (
                schema_name
                not in (exclude_schemas or [])  # user defined. Deprecated for remove in 0.6.0
                and schema_name not in {"information_schema", None}
            )
        }

        # Solve resolution order
        try:
            transaction = connection.begin()
            sess = Session(bind=connection)
            ordered_entities: List[T] = solve_resolution_order(sess, entities)
        finally:
            sess.rollback()

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

            if entity.__class__ not in allowed_entity_types:
                continue

            if not include_entity(entity, autogen_context, reflected=False):
                logger.debug(
                    "Ignoring local entity %s %s due to AutogenContext filters",
                    entity.__class__.__name__,
                    entity.identity,
                )
                continue

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
                    logger.debug(
                        "Detected NoOp op for %s %s",
                        entity.__class__.__name__,
                        entity.identity,
                    )

            finally:
                sess.rollback()

        # Required migration OPs, Drop
        try:
            # Start a parent transaction
            # Bind the session within the parent transaction
            transaction = connection.begin()
            sess = Session(bind=connection)

            # All database entities currently live
            # Check if anything needs to drop
            subclasses = collect_subclasses(alembic_utils, ReplaceableEntity)
            for entity_class in subclasses:

                if entity_class not in allowed_entity_types:
                    continue

                # Entities within the schemas that are live
                for schema in observed_schemas:

                    db_entities: List[ReplaceableEntity] = entity_class.from_database(
                        sess, schema=schema
                    )

                    # Check for functions that were deleted locally
                    for db_entity in db_entities:

                        if not include_entity(db_entity, autogen_context, reflected=True):
                            logger.debug(
                                "Ignoring remote entity %s %s due to AutogenContext filters",
                                db_entity.__class__.__name__,
                                db_entity.identity,
                            )
                            continue

                        for local_entity in local_entities:
                            if db_entity.identity == local_entity.identity:
                                break
                        else:
                            # No match was found locally
                            # If the entity passes the filters,
                            # we should create a DropOp
                            upgrade_ops.ops.append(DropOp(db_entity))
                            logger.info(
                                "Detected DropOp op for %s %s",
                                db_entity.__class__.__name__,
                                db_entity.identity,
                            )

        finally:
            sess.rollback()


def include_entity(entity: T, autogen_context: AutogenContext, reflected: bool) -> bool:
    """The functions on the AutogenContext object
    are described here:
    https://alembic.sqlalchemy.org/en/latest/api/autogenerate.html#alembic.autogenerate.api.AutogenContext.run_name_filters

    The meaning of the function parameters are explained in the corresponding
    definitions in the EnvironmentContext object:
    https://alembic.sqlalchemy.org/en/latest/api/runtime.html#alembic.runtime.environment.EnvironmentContext.configure.params.include_name

    This will only have an impact for projects which set include_object and/or include_name in the configuration
    of their Alembic env.
    """
    name = f"{entity.schema}.{entity.signature}"
    parent_names = {
        "schema_name": entity.schema,
        # At the time of writing, the implementation of `run_name_filters` in Alembic assumes that every type of object
        # will either be a table or have a table_name in its `parent_names` dict. This is true for columns and indexes,
        # but not true for the type of objects supported in this library such as views as functions. Nevertheless, to avoid
        # a KeyError when calling `run_name_filters`, we have to set some value.
        "table_name": f"Not applicable for type {entity.type_}",
    }
    # According to the Alembic docs, the name filter is only relevant for reflected objects
    if reflected:
        name_result = autogen_context.run_name_filters(name, entity.type_, parent_names)
    else:
        name_result = True

    # Object filters should be applied to object from local metadata and to reflected objects
    object_result = autogen_context.run_object_filters(
        entity, name, entity.type_, reflected=reflected, compare_to=None
    )
    return name_result and object_result
