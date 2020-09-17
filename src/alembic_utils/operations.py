from __future__ import annotations

import logging
from typing import List, Optional, TypeVar

from alembic.autogenerate import comparators, renderers
from alembic.operations import Operations
from flupy import flu

from alembic_utils.base import Entity
from alembic_utils.exceptions import DuplicateRegistration
from alembic_utils.reversible_op import ReversibleOp

from .base import Entity
from .reversible_op import ReversibleOp

log = logging.getLogger(__name__)

T = TypeVar("T", bound="Entity")

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
    target: Entity = operation.target
    operations.execute(target.to_sql_statement_create())


@Operations.implementation_for(DropOp)
def drop_function(operations, operation):
    target: Entity = operation.target
    operations.execute(target.to_sql_statement_drop())


@Operations.implementation_for(ReplaceOp)
@Operations.implementation_for(RevertOp)
def replace_or_revert_function(operations, operation):
    target: Entity = operation.target
    if target.is_replaceable:
        operations.execute(target.to_sql_statement_create_or_replace())
    else:
        operations.execute(target.to_sql_statement_drop())
        operations.execute(target.to_sql_statement_create())


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
    """Collect the function definition currently live in the database and use its definition
    as the downgrade revert target"""
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
                for entity_class in Entity.__subclasses__():
                    db_entities = entity_class.from_database(connection, schema=schema)

                    # Check for functions that were deleted locally
                    for db_entity in db_entities:
                        for local_entity in entities:
                            if db_entity.is_equal_identity(local_entity, connection):
                                break
                        else:
                            # No match was found locally
                            upgrade_ops.ops.append(DropOp(db_entity))
