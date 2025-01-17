import importlib
from pathlib import Path
from types import ModuleType
from typing import Generator, List, Type, TypeVar

from flupy import walk_files

T = TypeVar("T")


def walk_modules(module: ModuleType) -> Generator[ModuleType, None, None]:
    """Recursively yield python import paths to submodules in *module*

    Example:
        module_iter = walk_modules(alembic_utils)

        for module_path in module_iter:
            print(module_path)

        # alembic_utils.exceptions
        # alembic_utils.on_entity_mixin
        # ...
    """
    top_module = module
    top_path = Path(top_module.__path__[0])
    top_path_absolute = top_path.resolve()

    directories = (
        walk_files(str(top_path_absolute))
        .filter(lambda x: x.endswith(".py"))
        .map(Path)
        .group_by(lambda x: x.parent)
        .map(lambda x: (x[0], x[1].collect()))
    )

    for base_path, files in directories:
        # ensure this directory is a package
        if str(base_path / "__init__.py") in [str(f) for f in files]:
            for py_file in files:
                if py_file.name == "__init__.py":
                    continue

                # build the import path by taking the relative path to the top-level package
                relative = py_file.relative_to(top_path).with_suffix("")
                import_path = ".".join([module.__name__] + list(relative.parts))

                yield importlib.import_module(import_path)


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
    """Collect all subclasses of *class_* currently imported or defined in *module*

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

    imported: List[Type[T]] = list(class_.__subclasses__())  # type: ignore

    return list(set(found + imported))
