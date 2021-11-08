from typing import TypeVar

import flupy
from flupy import fluent
from flupy.cli import cli

import alembic_utils
from alembic_utils.experimental._collect_instances import (
    T,
    collect_instances,
    walk_modules,
)


def test_walk_modules() -> None:

    all_modules = [x for x in walk_modules(flupy)]
    assert fluent in all_modules


def test_walk_modules_exclude() -> None:

    all_modules = [x for x in walk_modules(flupy, exclude_modules=["flupy.fluent"])]
    assert fluent not in all_modules
    assert cli in all_modules


def test_collect_instances() -> None:

    instances = collect_instances(alembic_utils, TypeVar)
    assert T in instances
