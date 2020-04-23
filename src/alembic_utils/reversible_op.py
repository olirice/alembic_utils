from alembic.operations import MigrateOperation


class ReversibleOp(MigrateOperation):
    """A SQL operation that can be reversed"""

    def __init__(self, target):
        self.target = target

    @classmethod
    def invoke_for_target(cls, operations, target):
        op = cls(target)
        return operations.invoke(op)

    def reverse(self):
        raise NotImplementedError()
