from typing import TYPE_CHECKING, Type

from alembic.autogenerate import renderers
from alembic.operations import MigrateOperation, Operations
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
    for stmt in target.to_sql_statement_create_or_replace():
        operations.execute(stmt)


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
    # At the time is call is made, the engine is disconnected

    # We should never reach this call unless an update's revert is being rendered
    # In that case, get_required_migration_op  has cached the database's liver version
    # as target._version_to_replace

    target = op.target
    autogen_context.imports.add(target.render_import_statement())

    db_target = target._version_to_replace

    if db_target is None:
        raise UnreachableException

    variable_name = db_target.to_variable_name()
    return db_target.render_self_for_migration() + f"op.replace_entity({variable_name})"
