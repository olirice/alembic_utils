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
    <a href="https://github.com/olirice/alembic_utils/blob/master/LICENSE"><img src="https://img.shields.io/pypi/l/markdown-subtemplate.svg" alt="License" height="18"></a>
    <a href="https://badge.fury.io/py/alembic_utils"><img src="https://badge.fury.io/py/alembic_utils.svg" alt="PyPI version" height="18"></a>
    <a href="https://github.com/psf/black">
        <img src="https://img.shields.io/badge/code%20style-black-000000.svg" alt="Codestyle Black" height="18">
    </a>
    <a href="https://pypi.org/project/alembic_utils/"><img src="https://img.shields.io/pypi/dm/alembic_utils.svg" alt="Download count" height="18"></a>
</p>
<p>
    <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.6+-blue.svg" alt="Python version" height="18"></a>
    <a href=""><img src="https://img.shields.io/badge/postgresql-11+-blue.svg" alt="PostgreSQL version" height="18"></a>
</p>

----

**Documentation**: <a href="https://olirice.github.io/alembic_utils" target="_blank">https://olirice.github.io/alembic_utils</a>

**Source Code**: <a href="https://github.com/olirice/alembic_utils" target="_blank">https://github.com/olirice/alembic_utils</a>

---
[Alembic](https://alembic.sqlalchemy.org/en/latest/) is the defacto migration tool for use with [SQLAlchemy](https://www.sqlalchemy.org/). Without extensions, alembic can detect local changes to SQLAlchemy models and autogenerate a database migration or "revision" script. That revision can be applied to update the database's schema to match the SQLAlchemy model definitions.

Alembic Utils is an extension to alembic that adds support for autogenerating a larger number of [PostgreSQL](https://www.postgresql.org/) entity types, including [functions](https://www.postgresql.org/docs/current/sql-createfunction.html), [views](https://www.postgresql.org/docs/current/sql-createview.html), [materialized views](https://www.postgresql.org/docs/current/sql-creatematerializedview.html), [triggers](https://www.postgresql.org/docs/current/sql-createtrigger.html), and [policies](https://www.postgresql.org/docs/current/sql-createpolicy.html).


Visit the [quickstart guide](quickstart.md) for usage instructions.

<p align="center">&mdash;&mdash;  &mdash;&mdash;</p>
