import os
import re

from setuptools import find_packages, setup


def readme_to_long_description():
    """Convert README.md to long description for PYPI"""
    with open("README.md", encoding="UTF-8") as readme_file:
        readme = readme_file.readlines()

    readme = [line for ix, line in enumerate(readme) if ix not in range(2, 19)]
    long_description = "".join(readme)
    return long_description


def get_version(package):
    """
    Return package version as listed in `__version__` in `init.py`.
    """
    with open(os.path.join("src", package, "__init__.py")) as f:
        return re.search("__version__ = ['\"]([^'\"]+)['\"]", f.read()).group(1)


DEV_REQUIRES = [
    "black",
    "pylint",
    "pre-commit",
    "mypy",
    "sqlalchemy-stubs",
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
    long_description=readme_to_long_description(),
    long_description_content_type="text/markdown",
    python_requires=">=3.7",
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=["alembic", "psycopg2-binary", "flupy", "sqlalchemy", "parse"],
    extras_require={
        "dev": DEV_REQUIRES,
        "nvim": ["neovim", "python-language-server"],
        "docs": ["mkdocs", "pygments", "pymdown-extensions"],
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
