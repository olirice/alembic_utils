from alembic_utils.statement import coerce_to_unquoted


class OnEntityMixin:
    """Mixin to ReplaceableEntity providing setup for entity types requiring an "ON" clause"""

    def __init__(self, schema: str, signature: str, definition: str, on_entity: str = None):
        super().__init__(schema=schema, signature=signature, definition=definition)
        self.on_entity = coerce_to_unquoted(on_entity)

    def render_self_for_migration(self, omit_definition=False) -> str:
        """Render a string that is valid python code to reconstruct self in a migration"""
        var_name = self.to_variable_name()
        class_name = self.__class__.__name__
        escaped_definition = self.definition if not omit_definition else "# not required for op"

        return f"""{var_name} = {class_name}(
            schema="{self.schema}",
            signature="{self.signature}",
            on_entity="{self.on_entity}",
            definition={repr(escaped_definition)}
        )\n\n"""
