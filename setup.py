import os
import re

from setuptools import find_packages, setup


def get_version(package):
    """
    Return package version as listed in `__version__` in `__init__.py`.
    """
    with open(os.path.join("src", package, "__init__.py")) as f:
        return re.search("__version__ = ['\"]([^'\"]+)['\"]", f.read()).group(1)


DEV_REQUIRES = [
    "black",
    "pylint",
    "pre-commit",
    "mypy",
    "sqlalchemy-stubs",
    "psycopg2-binary",
    "pytest",
    "pytest-cov",
    "mkdocs",
]

setup(
    name="alembic_utils",
    version=get_version("alembic_utils"),
    author="Oliver Rice",
    author_email="oliver@oliverrice.com",
    license="MIT",
    description="A sqlalchemy/alembic extension for migrating procedures and views ",
    python_requires=">=3.6",
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=[
        "alembic>=1.5.7",
        "flupy",
        "sqlalchemy>=1.3.0",
        "parse",
        "typing_extensions",
    ],
    extras_require={
        "dev": DEV_REQUIRES,
        "nvim": ["neovim", "python-language-server"],
        "docs": ["mkdocs", "pygments", "pymdown-extensions", "mkautodoc"],
    },
    include_package_data=True,
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: SQL",
    ],
)
