import importlib
from pathlib import Path
from types import ModuleType
from typing import Generator, List, Type, TypeVar

from flupy import walk_files

T = TypeVar("T")


def walk_modules(module: ModuleType) -> Generator[ModuleType, None, None]:
    """Recursively yield python import paths to submodules in *module*

    Example:
        import alembic_utils
        module_iter = iter_module_pathes(alembic_utils)

        for module_path in module_iter:
            print(module_path)

        # alembic_utils.exceptions
        # alembic_utils.on_entity_mixin
        # ...
    """
    top_module = module
    top_path = top_module.__path__[0]  # type: ignore

    directories = (
        walk_files(str(top_path))
        .filter(lambda x: x.endswith(".py"))
        .map(Path)
        .group_by(lambda x: x.parent)
        .map(lambda x: (x[0], x[1].collect()))
    )

    for base_path, files in directories:
        if str(base_path / "__init__.py") in [str(x) for x in files]:
            for module_path in files:
                if "__init__.py" not in str(module_path):

                    # Example: elt.settings
                    module_import_path = str(module_path)[
                        len(str(top_path)) - len(top_module.__name__) :
                    ].replace("/", ".")[:-3]

                    module = importlib.import_module(module_import_path)
                    yield module


def collect_instances(module: ModuleType, class_: Type[T]) -> List[T]:
    """Collect all instances of *class_* defined in *module*

    Note: Will import all submodules in *module*. Beware of import side effects
    """

    found: List[T] = []

    for module_ in walk_modules(module):

        for _, variable in module_.__dict__.items():

            if isinstance(variable, class_):
                # Ensure variable is not a subclass
                if variable.__class__ == class_:
                    found.append(variable)
    return found


def collect_subclasses(module: ModuleType, class_: Type[T]) -> List[Type[T]]:
    """Collect all subclasses of *class_* defined in *module*

    Note: Will import all submodules in *module*. Beware of import side effects
    """

    found: List[Type[T]] = []

    for module_ in walk_modules(module):

        for _, variable in module_.__dict__.items():
            try:
                if issubclass(variable, class_) and not class_ == variable:
                    found.append(variable)
            except TypeError:
                # argument 2 to issubclass must be a class ....
                pass

    return list(set(found))
