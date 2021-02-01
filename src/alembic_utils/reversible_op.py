from typing import TYPE_CHECKING, Type

from alembic.operations import MigrateOperation
from typing_extensions import Protocol

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
