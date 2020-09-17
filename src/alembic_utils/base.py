from __future__ import annotations

from abc import abstractmethod
from typing import Optional

from alembic_utils.reversible_op import ReversibleOp

# from typing_extensions import Protocol
from .reversible_op import ReversibleOp


class Entity:
    """A SQL Entity that can be replaced"""

    @abstractmethod
    def to_sql_statement_create(self) -> str:
        """ Generates a SQL "create function" statement for PGFunction """
        raise NotImplementedError

    @abstractmethod
    def to_sql_statement_drop(self) -> str:
        """ Generates a SQL "drop function" statement for PGFunction """
        raise NotImplementedError

    @abstractmethod
    def to_sql_statement_create_or_replace(self) -> str:
        """ Generates a SQL "create or replace function" statement for PGFunction """
        raise NotImplementedError

    @abstractmethod
    def get_required_migration_op(self, connection) -> Optional[ReversibleOp]:
        """Get the migration operation required for autogenerate"""
        raise NotImplementedError

    @abstractmethod
    def render_self_for_migration(self, omit_definition=False) -> str:
        """Render a string that is valid python code to reconstruct self in a migration"""
        raise NotImplementedError

    @classmethod
    def render_import_statement(cls) -> str:
        """Render a string that is valid python code to import current class"""
        module_path = cls.__module__
        class_name = cls.__name__
        return f"from {module_path} import {class_name}\nfrom sqlalchemy import text as sql_text"
