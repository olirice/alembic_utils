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

---

**Documentation**: <a href="https://olirice.github.io/alembic_utils" target="_blank">https://olirice.github.io/alembic_utils</a>

**Source Code**: <a href="https://github.com/olirice/alembic_utils" target="_blank">https://github.com/olirice/alembic_utils</a>

---

**Autogenerate Support for PostgreSQL Functions, Views, Materialized View, Triggers, and Policies**

[Alembic](https://alembic.sqlalchemy.org/en/latest/) is the defacto migration tool for use with [SQLAlchemy](https://www.sqlalchemy.org/). Without extensions, alembic can detect local changes to SQLAlchemy models and autogenerate a database migration or "revision" script. That revision can be applied to update the database's schema to match the SQLAlchemy model definitions.

Alembic Utils is an extension to alembic that adds support for autogenerating a larger number of [PostgreSQL](https://www.postgresql.org/) entity types, including [functions](https://www.postgresql.org/docs/current/sql-createfunction.html), [views](https://www.postgresql.org/docs/current/sql-createview.html), [materialized views](https://www.postgresql.org/docs/current/sql-creatematerializedview.html), [triggers](https://www.postgresql.org/docs/current/sql-createtrigger.html), and [policies](https://www.postgresql.org/docs/current/sql-createpolicy.html).

### TL;DR

Update alembic's `env.py` to register a function or view:

```python
# migrations/env.py
from alembic_utils.pg_function import PGFunction
from alembic_utils.replaceable_entity import register_entities


to_upper = PGFunction(
  schema='public',
  signature='to_upper(some_text text)'
  definition="""
  RETURNS text as
  $$
    SELECT upper(some_text)
  $$ language SQL;
  """
)

register_entities([to_upper])
```

You're done!

The next time you autogenerate a revision with
```shell
alembic revision --autogenerate -m 'create to_upper'
```
Alembic will detect if your entities are new, updated, or removed & populate the revison's `upgrade` and `downgrade` sections automatically.

For example:

```python
"""create to_upper

Revision ID: 8efi0da3a4
Revises:
Create Date: 2020-04-22 09:24:25.556995
"""
from alembic import op
import sqlalchemy as sa
from alembic_utils.pg_function import PGFunction

# revision identifiers, used by Alembic.
revision = '8efi0da3a4'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    public_to_upper_6fa0de = PGFunction(
        schema="public",
        signature="to_upper(some_text text)",
        definition="""
        returns text
        as
        $$ select upper(some_text) $$ language SQL;
        """
    )

    op.create_entity(public_to_upper_6fa0de)


def downgrade():
    public_to_upper_6fa0de = PGFunction(
        schema="public",
        signature="to_upper(some_text text)",
        definition="# Not Used"
    )

    op.drop_entity(public_to_upper_6fa0de)
```


Visit the [quickstart guide](https://olirice.github.io/alembic_utils/quickstart/) for usage instructions.

<p align="center">&mdash;&mdash;  &mdash;&mdash;</p>
