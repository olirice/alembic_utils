from typing import TYPE_CHECKING, Optional, Type

from alembic.autogenerate import renderers
from alembic.operations import MigrateOperation, Operations
from sqlalchemy.orm import Session
from typing_extensions import Protocol

from alembic_utils.exceptions import UnreachableException

if TYPE_CHECKING:
    from alembic_utils.replaceable_entity import ReplaceableEntity


class SupportsTarget(Protocol):
    def __init__(self, target: "ReplaceableEntity") -> None:
        pass


class SupportsTargetCascade(Protocol):
    def __init__(self, target: "ReplaceableEntity", cascade: bool) -> None:
        pass


class ReversibleOp(MigrateOperation):
    """A SQL operation that can be reversed"""

    def __init__(self, target: "ReplaceableEntity"):
        self.target = target

    @classmethod
    def invoke_for_target(cls: Type[SupportsTarget], operations, target: "ReplaceableEntity"):
        op = cls(target)
        return operations.invoke(op)

    @classmethod
    def invoke_for_target_optional_cascade(
        cls: Type[SupportsTargetCascade], operations, target: "ReplaceableEntity", cascade=False
    ):
        op = cls(target, cascade=cascade)  # pylint: disable=unexpected-keyword-arg
        return operations.invoke(op)

    def reverse(self):
        raise NotImplementedError()


##############
# Operations #
##############


@Operations.register_operation("create_entity", "invoke_for_target")
class CreateOp(ReversibleOp):
    def reverse(self):
        return DropOp(self.target)


@Operations.register_operation("drop_entity", "invoke_for_target_optional_cascade")
class DropOp(ReversibleOp):
    def __init__(self, target: "ReplaceableEntity", cascade: bool = False) -> None:
        self.cascade = cascade
        super().__init__(target)

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
def create_entity(operations, operation):
    target: "ReplaceableEntity" = operation.target
    operations.execute(target.to_sql_statement_create())


@Operations.implementation_for(DropOp)
def drop_entity(operations, operation):
    target: "ReplaceableEntity" = operation.target
    operations.execute(target.to_sql_statement_drop(cascade=operation.cascade))


@Operations.implementation_for(ReplaceOp)
@Operations.implementation_for(RevertOp)
def replace_or_revert_entity(operations, operation):
    target: "ReplaceableEntity" = operation.target
    operations.execute(target.to_sql_statement_create_or_replace())


##########
# Render #
##########


@renderers.dispatch_for(CreateOp)
def render_create_entity(autogen_context, op):
    target = op.target
    autogen_context.imports.add(target.render_import_statement())
    variable_name = target.to_variable_name()
    return target.render_self_for_migration() + f"op.create_entity({variable_name})\n"


@renderers.dispatch_for(DropOp)
def render_drop_entity(autogen_context, op):
    target = op.target
    autogen_context.imports.add(target.render_import_statement())
    variable_name = target.to_variable_name()
    return (
        target.render_self_for_migration(omit_definition=False)
        + f"op.drop_entity({variable_name})\n"
    )


@renderers.dispatch_for(ReplaceOp)
def render_replace_entity(autogen_context, op):
    target = op.target
    autogen_context.imports.add(target.render_import_statement())
    variable_name = target.to_variable_name()
    return target.render_self_for_migration() + f"op.replace_entity({variable_name})\n"


@renderers.dispatch_for(RevertOp)
def render_revert_entity(autogen_context, op):
    """Collect the entity definition currently live in the database and use its definition
    as the downgrade revert target"""

    # Note: There can be differences in the local definition's identity property and the
    # corresponding record in the database. To avoid that issue, we first retrieve the
    # local definition's corresponding definition once its been rendered in the db
    # and then use that identity property to find the existing definition.

    # We should never reach this function call unless there is an existing definition
    # so failing to find a match results in an unreachable exception

    target = op.target
    autogen_context.imports.add(target.render_import_statement())

    context = autogen_context

    # The connection autogen_context engine is closed
    engine = autogen_context.connection.engine

    with engine.connect() as connection:
        sess = Session(bind=connection)

        db_def = target.get_database_definition(sess)

        db_target: Optional["ReplaceableEntity"] = None

        db_entities = sorted(
            db_def.from_database(sess, schema=db_def.schema), key=lambda x: x.identity
        )
        for existing in db_entities:
            if existing.identity == db_def.identity:
                db_target = existing
                break

        if db_target is None:
            raise UnreachableException

    variable_name = db_target.to_variable_name()
    return db_target.render_self_for_migration() + f"op.replace_entity({variable_name})"
