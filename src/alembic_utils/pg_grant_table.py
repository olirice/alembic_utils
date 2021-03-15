import warnings

from .pg_grant_columns import PGGrantColumns

warnings.warn(
    "PGGrantTable has been renamed PGGrantColumns and is available at alembic_utils.pg_grant_columns.PGGrantColumns. alembic_utils.pg_grant_table.PGGrantTable will be removed in version 0.6.0 please update all references in historic migrations and source.",
    DeprecationWarning,
)


PGGrantTable = PGGrantColumns


__all__ = ["PGGrantTable"]
