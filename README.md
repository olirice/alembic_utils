# Alembic Utils

<p>
    <a href="https://github.com/olirice/alembic_utils/actions">
        <img src="https://github.com/olirice/alembic_utils/workflows/Tests/badge.svg" alt="Test Status" height="18">
    </a>
    <a href="https://github.com/olirice/alembic_utils/actions">
        <img src="https://github.com/olirice/alembic_utils/workflows/pre-commit%20hooks/badge.svg" alt="Pre-commit Status" height="18">
    </a>
    <a href="https://codecov.io/gh/olirice/alembic_utils"><img src="https://codecov.io/gh/olirice/alembic_utils/branch/master/graph/badge.svg" height="18"></a>
</p>
<p>
    <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.7+-blue.svg" alt="Python version" height="18"></a>
    <a href="https://github.com/olirice/alembic_utils/blob/master/LICENSE"><img src="https://img.shields.io/pypi/l/markdown-subtemplate.svg" alt="License" height="18"></a>
    <a href="https://badge.fury.io/py/alembic_utils"><img src="https://badge.fury.io/py/alembic_utils.svg" alt="PyPI version" height="18"></a>
    <a href="https://github.com/psf/black">
        <img src="https://img.shields.io/badge/code%20style-black-000000.svg" alt="Codestyle Black" height="18">
    </a>
</p>

---

**Documentation**: <a href="https://olirice.github.io/alembic_utils" target="_blank">https://olirice.github.io/alembic_utils</a>

**Source Code**: <a href="https://github.com/olirice/alembic_utils" target="_blank">https://github.com/olirice/alembic_utils</a>

---

**Autogenerate Support for PostgreSQL Functions and (soon) Views**

[Alembic](https://alembic.sqlalchemy.org/en/latest/) is the defacto migration tool for usage with [SQLAlchemy](https://www.sqlalchemy.org/). Without extensions, alembic can detect local changes to SQLAlchemy models and autogenerate a database migration or "revision" script. That revision can be applied to update the database's schema to match the SQLAlchemy model definitions.

Alembic Utils is an extension to alembic that adds autogeneration support for [PostgreSQL](https://www.postgresql.org/) functions and (soon) views.

Visit the [quickstart guide](https://olirice.github.io/alembic_utils/quickstart/) for usage instructions.

<p align="center">&mdash;&mdash;  &mdash;&mdash;</p>
