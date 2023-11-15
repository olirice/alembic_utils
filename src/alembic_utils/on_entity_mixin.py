from typing import TYPE_CHECKING

from alembic_utils.statement import coerce_to_unquoted

if TYPE_CHECKING:
    from alembic_utils.replaceable_entity import ReplaceableEntity

    _Base = ReplaceableEntity
else:
    _Base = object


class OnEntityMixin(_Base):
    """Mixin to ReplaceableEntity providing setup for entity types requiring an "ON" clause"""

    def __init__(self, signature: str, definition: str, on_entity: str, schema: str = "public"):
        if "." not in on_entity:
            schema = "public"
        else:
            schema = on_entity.split(".")[0]

        self.include_schema_prefix: bool = schema != "public"
        super().__init__(schema=schema, signature=signature, definition=definition)

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

        code: str = f"{var_name} = {class_name}("
        if self.schema and self.include_schema_prefix:
            code += f'\n    schema="{self.schema}",'
        code += f'\n    signature="{self.signature}",'
        code += f'\n    on_entity="{self.on_entity}",'
        code += f'\n    definition={repr(escaped_definition)},'
        code += '\n)\n'
        return code

    def to_variable_name(self) -> str:
        """A deterministic variable name based on PGFunction's contents"""
        schema_name = self.schema.lower() + "_" if self.schema and self.include_schema_prefix else ""
        object_name = self.signature.split("(")[0].strip().lower()
        _, _, unqualified_entity_name = self.on_entity.lower().partition(".")
        return f"{schema_name}{unqualified_entity_name}_{object_name}"
