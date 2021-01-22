from typing import TYPE_CHECKING

from alembic_utils.statement import coerce_to_unquoted

if TYPE_CHECKING:
    from alembic_utils.replaceable_entity import ReplaceableEntity

    _Base = ReplaceableEntity
else:
    _Base = object


class OnEntityMixin(_Base):
    """Mixin to ReplaceableEntity providing setup for entity types requiring an "ON" clause"""

    def __init__(self, schema: str, signature: str, definition: str, on_entity: str):
        super().__init__(schema=schema, signature=signature, definition=definition)

        if "." not in on_entity:
            on_entity = "public." + on_entity

        # Guarenteed to have a schema
        self.on_entity = coerce_to_unquoted(on_entity)

    @property
    def identity(self) -> str:
        """A string that consistently and globally identifies a function

        Overriding default to add the "on table" clause
        """
        return f"{self.__class__.__name__}: {self.schema}.{self.signature} {self.on_entity}"

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
)\n"""

    def to_variable_name(self) -> str:
        """A deterministic variable name based on PGFunction's contents """
        schema_name = self.schema.lower()
        object_name = self.signature.split("(")[0].strip().lower()
        _, _, unqualified_entity_name = self.on_entity.lower().partition(".")
        return f"{schema_name}_{unqualified_entity_name}_{object_name}"
