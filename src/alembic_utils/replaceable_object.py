# pylint: disable=unused-argument,invalid-name,line-too-long
from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Type, TypeVar

from alembic.autogenerate import renderers
from alembic.operations import Operations

from alembic_utils.reversible_op import ReversibleOp

T = TypeVar("T", bound="ReplaceableObject")


def normalize_whitespace(text, base_whitespace: str = " ") -> str:
    """ Convert all whitespace to *base_whitespace* """
    return base_whitespace.join(text.split()).strip()


class ReplaceableObject:
    """A SQL Entity that can be replaced"""

    def __init__(self, schema: str, signature: str, definition: str):
        self.schema: str = normalize_whitespace(schema)
        self.signature: str = normalize_whitespace(signature)
        self.definition: str = definition.strip()

    @classmethod
    def from_sql(cls: Type[T], sql: str) -> Optional[T]:
        """Create an instance from a SQL string"""
        raise NotImplementedError()

    @classmethod
    def from_database(cls, connection, schema="%") -> List[T]:
        """Collect existing entities from the database for given schema"""
        raise NotImplementedError()

    def to_sql_statement_create(self) -> str:
        """ Generates a SQL "create function" statement for PGFunction """
        raise NotImplementedError()

    def to_sql_statement_drop(self) -> str:
        """ Generates a SQL "drop function" statement for PGFunction """
        raise NotImplementedError()

    def to_sql_statement_create_or_replace(self) -> str:
        """ Generates a SQL "create or replace function" statement for PGFunction """
        raise NotImplementedError()

    def get_database_definition(self: T, connection) -> Optional[T]:
        """ Looks up self and return the copy existing in the database (maybe)the"""
        all_entities = self.from_database(connection, schema=self.schema)

        matches = [x for x in all_entities if self.is_equal_identity(x)]
        if len(matches) == 0:
            return None
        db_match = matches[0]
        return db_match

    def render_self_for_migration(self) -> str:
        """Render a string that is valid python code to reconstruct self in a migration"""
        var_name = self.to_variable_name()
        class_name = self.__class__.__name__
        return f"""{var_name} = {class_name}(
            schema="{self.schema}",
            signature="{self.signature}",
            definition=\"\"\"{self.definition}\"\"\"
        )\n\n"""

    @classmethod
    def render_import_statement(cls) -> str:
        """Render a string that is valid python code to import current class"""
        module_path = cls.__module__
        class_name = cls.__name__
        return f"from {module_path} import {class_name}"

    @property
    def identity(self) -> str:
        """A string that consistently and globally identifies a function"""
        return f"{self.schema}.{self.signature}"

    @classmethod
    def from_path(cls: Type[T], path: Path) -> Optional[T]:
        """Create an instance from a sql file path"""
        with path.open() as sql_file:
            sql = sql_file.read()
        return cls.from_sql(sql)

    def to_variable_name(self):
        """A deterministic variable name based on PGFunction's contents """
        schema_name = self.schema.lower()
        object_name = self.signature.split("(")[0].strip().lower()
        return f"{schema_name}_{object_name}"

    def is_equal_identity(self, other: T) -> bool:
        """ Is the signature of self and other the same """
        return self.identity == other.identity

    def is_equal_definition(self: T, other: T) -> bool:
        """ Is the definition within self and other the same """
        raise NotImplementedError()

    def get_required_migration_op(self, connection) -> Optional[ReversibleOp]:
        """Get the migration operation required for autogenerate"""
        # All entities in the database for self's schema
        entities_in_database = self.from_database(connection, schema=self.schema)

        found_identical = any([self.is_equal_definition(x) for x in entities_in_database])

        found_signature = any([self.is_equal_identity(x) for x in entities_in_database])

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
    target: ReplaceableObject = operation.target
    operations.execute(target.to_sql_statement_create())


@Operations.implementation_for(DropOp)
def drop_function(operations, operation):
    target: ReplaceableObject = operation.target
    operations.execute(target.to_sql_statement_drop())


@Operations.implementation_for(ReplaceOp)
@Operations.implementation_for(RevertOp)
def replace_or_revert_function(operations, operation):
    target: ReplaceableObject = operation.target
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
    return target.render_self_for_migration() + f"op.drop_entity({variable_name})"


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
