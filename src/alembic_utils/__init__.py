import os
from pathlib import Path

from alembic_utils.pg_function import PGFunction

REPO_ROOT = Path(os.path.abspath(os.path.dirname(__file__))).parent.parent.resolve()
TEST_RESOURCE_ROOT = REPO_ROOT / "src" / "test" / "resources"
TEST_VERSIONS_ROOT = REPO_ROOT / "src" / "test" / "alembic_config" / "versions"


__all__ = ["PGFunction", "REPO_ROOT", "TEST_VERSIONS_ROOT", "TEST_RESOURCE_ROOT"]
