# pylint: disable=unsupported-assignment-operation
import contextlib
import os
from io import StringIO
from pathlib import Path
from typing import Any, Callable, Dict, NoReturn

from alembic import command as alem_command
from alembic.autogenerate.compare import comparators
from alembic.config import Config
from sqlalchemy.engine import Engine

REPO_ROOT = Path(os.path.abspath(os.path.dirname(__file__))).parent.parent.resolve()
TEST_RESOURCE_ROOT = REPO_ROOT / "src" / "test" / "resources"
TEST_VERSIONS_ROOT = REPO_ROOT / "src" / "test" / "alembic_config" / "versions"


ALEMBIC_COMMAND_MAP: Dict[str, Callable[..., NoReturn]] = {
    "upgrade": alem_command.upgrade,
    "downgrade": alem_command.downgrade,
    "revision": alem_command.revision,
    "current": alem_command.current,
}


def build_alembic_config(engine: Engine) -> Config:
    """Populate alembic configuration from metadata and config file."""
    path_to_alembic_ini = REPO_ROOT / "alembic.ini"

    alembic_cfg = Config(path_to_alembic_ini)
    # Make double sure alembic references the test database
    alembic_cfg.set_main_option("sqlalchemy.url", str(engine.url))
    alembic_cfg.set_main_option("script_location", str((Path("src") / "test" / "alembic_config")))
    return alembic_cfg


def run_alembic_command(engine: Engine, command: str, command_kwargs: Dict[str, Any]) -> str:
    command_func = ALEMBIC_COMMAND_MAP[command]

    stdout = StringIO()

    alembic_cfg = build_alembic_config(engine)
    with engine.begin() as connection:
        alembic_cfg.attributes["connection"] = connection
        with contextlib.redirect_stdout(stdout):
            command_func(alembic_cfg, **command_kwargs)
    return stdout.getvalue()


def reset_event_listener_registry() -> None:
    """Resets event listeners watching for changes when --autogenerate is used with revisions"""
    comparators._registry = {
        (target, qualifier): [func for func in funcs if "entities" not in func.__name__]
        for (target, qualifier), funcs in comparators._registry.items()
    }
