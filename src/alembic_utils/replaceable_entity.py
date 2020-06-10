# pylint: disable=unused-argument,invalid-name,line-too-long
from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import List, Optional, Tuple, Type, TypeVar

from alembic.autogenerate import comparators, renderers
from alembic.operations import Operations
from flupy import flu

from alembic_utils.cache import cachedmethod
from alembic_utils.exceptions import DuplicateRegistration
from alembic_utils.reversible_op import ReversibleOp

log = logging.getLogger(__name__)

T = TypeVar("T", bound="ReplaceableEntity")


def normalize_whitespace(text, base_whitespace: str = " ") -> str:
    """ Convert all whitespace to *base_whitespace* """
    return base_whitespace.join(text.split()).strip()


@contextmanager
def simulate_entity(connection, entity):
    """Creates *entity* in the *dummy_schema* and self would be transformed into if it were created in the database"""
    dummy_schema = "alembic_utils"
    assert entity.schema == dummy_schema
    cls = entity.__class__
    adj_target = cls(dummy_schema, entity.signature, entity.definition)
    connection.execute(f"drop schema if exists {dummy_schema} cascade")
    connection.execute(f"create schema if not exists {dummy_schema}")
    try:
        connection.execute(adj_target.to_sql_statement_create())
        yield
    finally:
        connection.execute(f"drop schema if exists {dummy_schema} cascade")


class ReplaceableEntity:
    """A SQL Entity that can be replaced"""

    _CACHE = {}

    def __init__(self, schema: str, signature: str, definition: str):
        self.schema: str = normalize_whitespace(schema)
        self.signature: str = normalize_whitespace(signature)
        self.definition: str = definition.strip()

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
    def from_database(cls, connection, schema="%") -> List[T]:
        """Collect existing entities from the database for given schema"""
        raise NotImplementedError()

    def get_compare_identity_query(self) -> str:
        """Return SQL string that returns 1 row for existing DB object"""
        raise NotImplementedError()

    def get_compare_definition_query(self) -> str:
        """Return SQL string that returns 1 row for existing DB object"""
        raise NotImplementedError()

    @cachedmethod(
        lambda self: self._CACHE,
        key=lambda self, _: (
            self.__class__.__name__,
            "identity",
            self.schema,
            self.signature,
            self.definition,
        ),
    )
    def get_identity_comparable(self, connection) -> Tuple:
        """ Generates a SQL "create function" statement for PGFunction """
        # Create other in a dummy schema
        cls = self.__class__
        adj_self = cls("alembic_utils", self.signature, self.definition)
        identity_query = adj_self.get_compare_identity_query()
        with simulate_entity(connection, adj_self):
            # Collect the definition_comparable for dummy schema self
            row = (self.schema,) + tuple(connection.execute(identity_query).fetchone())
        return row

    @cachedmethod(
        lambda self: self._CACHE,
        key=lambda self, _: (
            self.__class__.__name__,
            "definition",
            self.schema,
            self.signature,
            self.definition,
        ),
    )
    def get_definition_comparable(self, connection) -> Tuple:
        """ Generates a SQL "create function" statement for PGFunction """
        # Create self in a dummy schema
        cls = self.__class__
        adj_self = cls("alembic_utils", self.signature, self.definition)
        definition_query = adj_self.get_compare_definition_query()
        with simulate_entity(connection, adj_self):
            # Collect the definition_comparable for dummy schema self
            row = (self.schema,) + tuple(connection.execute(definition_query).fetchone())
        return row

    def to_sql_statement_create(self) -> str:
        """ Generates a SQL "create function" statement for PGFunction """
        raise NotImplementedError()

    def to_sql_statement_drop(self) -> str:
        """ Generates a SQL "drop function" statement for PGFunction """
        raise NotImplementedError()

    def to_sql_statement_create_or_replace(self) -> str:
        """ Generates a SQL "create or replace function" statement for PGFunction """
        raise NotImplementedError()

    def is_equal_definition(self, other: T, connection) -> bool:
        """Is the definition within self and other the same"""
        self_comparable = self.get_definition_comparable(connection)
        other_comparable = other.get_definition_comparable(connection)
        return self_comparable == other_comparable

    def is_equal_identity(self, other: T, connection) -> bool:
        """Is the definition within self and other the same"""
        self_comparable = self.get_identity_comparable(connection)
        other_comparable = other.get_identity_comparable(connection)
        return self_comparable == other_comparable

    def get_database_definition(self: T, connection) -> Optional[T]:
        """ Looks up self and return the copy existing in the database (maybe)the"""
        all_entities = self.from_database(connection, schema=self.schema)
        matches = [x for x in all_entities if self.is_equal_identity(x, connection)]
        if len(matches) == 0:
            return None
        db_match = matches[0]
        return db_match

    def render_self_for_migration(self, omit_definition=False) -> str:
        """Render a string that is valid python code to reconstruct self in a migration"""
        var_name = self.to_variable_name()
        class_name = self.__class__.__name__
        escaped_definition = self.definition if not omit_definition else "# not required for op"
        return f"""{var_name} = {class_name}(
            schema="{self.schema}",
            signature="{self.signature}",
            definition={repr(escaped_definition)}
        )\n\n"""

    @classmethod
    def render_import_statement(cls) -> str:
        """Render a string that is valid python code to import current class"""
        module_path = cls.__module__
        class_name = cls.__name__
        return f"from {module_path} import {class_name}\nfrom sqlalchemy import text as sql_text"

    @property
    def identity(self) -> str:
        """A string that consistently and globally identifies a function"""
        return f"{self.schema}.{self.signature}"

    def to_variable_name(self):
        """A deterministic variable name based on PGFunction's contents """
        schema_name = self.schema.lower()
        object_name = self.signature.split("(")[0].strip().lower()
        return f"{schema_name}_{object_name}"

    def get_required_migration_op(self, connection) -> Optional[ReversibleOp]:
        """Get the migration operation required for autogenerate"""
        # All entities in the database for self's schema
        entities_in_database = self.from_database(connection, schema=self.schema)

        found_identical = any(
            [self.is_equal_definition(x, connection) for x in entities_in_database]
        )

        found_signature = any([self.is_equal_identity(x, connection) for x in entities_in_database])

        if found_identical:
            return None
        if found_signature:
            return ReplaceOp(self)
        return CreateOp(self)


##############
# Operations #
##############


@Operations.register_operation("create_entity", "invoke_for_target")
class CreateOp(ReversibleOp):
    def reverse(self):
        return DropOp(self.target)


@Operations.register_operation("drop_entity", "invoke_for_target")
class DropOp(ReversibleOp):
    def reverse(self):
        return CreateOp(self.target)


@Operations.register_operation("replace_entity", "invoke_for_target")
class ReplaceOp(ReversibleOp):
    def reverse(self):
        return RevertOp(self.target)


class RevertOp(ReversibleOp):
    # Revert is never in an upgrade, so no need to implement reverse
    pass


###################
# Implementations #
###################


@Operations.implementation_for(CreateOp)
def create_function(operations, operation):
    target: ReplaceableEntity = operation.target
    operations.execute(target.to_sql_statement_create())


@Operations.implementation_for(DropOp)
def drop_function(operations, operation):
    target: ReplaceableEntity = operation.target
    operations.execute(target.to_sql_statement_drop())


@Operations.implementation_for(ReplaceOp)
@Operations.implementation_for(RevertOp)
def replace_or_revert_function(operations, operation):
    target: ReplaceableEntity = operation.target
    operations.execute(target.to_sql_statement_create_or_replace())


##########
# Render #
##########


@renderers.dispatch_for(CreateOp)
def render_create_function(autogen_context, op):
    target = op.target
    autogen_context.imports.add(target.render_import_statement())
    variable_name = target.to_variable_name()
    return target.render_self_for_migration() + f"op.create_entity({variable_name})"


@renderers.dispatch_for(DropOp)
def render_drop_function(autogen_context, op):
    target = op.target
    autogen_context.imports.add(target.render_import_statement())
    variable_name = target.to_variable_name()
    return (
        target.render_self_for_migration(omit_definition=True) + f"op.drop_entity({variable_name})"
    )


@renderers.dispatch_for(ReplaceOp)
def render_replace_function(autogen_context, op):
    target = op.target
    autogen_context.imports.add(target.render_import_statement())
    variable_name = target.to_variable_name()
    return target.render_self_for_migration() + f"op.replace_entity({variable_name})"


@renderers.dispatch_for(RevertOp)
def render_revert_function(autogen_context, op):
    """ Collect the function definition currently live in the database and use its definition
    as the downgrade revert target """
    target = op.target
    autogen_context.imports.add(target.render_import_statement())

    context = autogen_context
    engine = context.connection.engine

    with engine.connect() as connection:
        db_target = op.target.get_database_definition(connection)

    variable_name = db_target.to_variable_name()
    return db_target.render_self_for_migration() + f"op.replace_entity({variable_name})"


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
        autogen_context, upgrade_ops, sqla_schemas: List[Optional[str]]
    ):
        engine = autogen_context.connection.engine

        # Ensure pg_functions have unique identities (not registered twice)
        for ident, function_group in flu(entities).group_by(key=lambda x: x.identity):
            if len(function_group.collect()) > 1:
                raise DuplicateRegistration(
                    f"PGFunction with identity {ident} was registered multiple times"
                )

        # User registered schemas + automatically registered schemas (from SQLA Metadata)
        observed_schemas: List[str] = []
        if schemas is not None:
            for schema in schemas:
                observed_schemas.append(schema)

        sqla_schemas = [schema for schema in sqla_schemas or [] if schema is not None]
        observed_schemas.extend(sqla_schemas)

        for entity in entities:
            observed_schemas.append(entity.schema)

        # Remove excluded schemas
        observed_schemas = [x for x in set(observed_schemas) if x not in (exclude_schemas or [])]

        with engine.connect() as connection:

            # Check for new or updated entities
            for local_entity in entities:
                maybe_op = local_entity.get_required_migration_op(connection)
                if maybe_op is not None:
                    if isinstance(maybe_op, CreateOp):
                        log.warning(
                            "Detected added entity %r.%r",
                            local_entity.schema,
                            local_entity.signature,
                        )
                    elif isinstance(maybe_op, ReplaceOp):
                        log.info(
                            "Detected updated entity %r.%r",
                            local_entity.schema,
                            local_entity.signature,
                        )
                    upgrade_ops.ops.append(maybe_op)

            # Entities grouped by class (e.g. PGFunction, PGView, etc)
            entity_groups = flu(entities).group_by(lambda x: x.__class__, sort=False)

            # Check if anything needs to drop
            for schema in observed_schemas:
                # Entities within the schemas that are live
                for entity_class in ReplaceableEntity.__subclasses__():
                    db_entities = entity_class.from_database(connection, schema=schema)

                    # Check for functions that were deleted locally
                    for db_entity in db_entities:
                        for local_entity in entities:
                            if db_entity.is_equal_identity(local_entity, connection):
                                break
                        else:
                            # No match was found locally
                            upgrade_ops.ops.append(DropOp(db_entity))
