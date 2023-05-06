## Quickstart

### Installation
*Requirements* Python 3.6+

First, install alembic_utils
```shell
$ pip install alembic_utils
```

Next, add "alembic_utils" to the logger keys in `alembic.ini` and a configuration for it.
```
...
[loggers]
keys=root,sqlalchemy,alembic,alembic_utils

[logger_alembic_utils]
level = INFO
handlers =
qualname = alembic_utils
```

### Reference

Then add a function to your project
```python
# my_function.py
from alembic_utils.pg_function import PGFunction

to_upper = PGFunction(
  schema='public',
  signature='to_upper(some_text text)',
  definition="""
  RETURNS text as
  $$
    SELECT upper(some_text)
  $$ language SQL;
  """
)
```

and/or a view
```python
# my_view.py
from alembic_utils.pg_view import PGView

first_view = PGView(
    schema="public",
    signature="first_view",
    definition="select * from information_schema.tables",
)

```



Finally, update your `<migrations_folder>/env.py` to register your entities with alembic_utils.

```python
# <migrations_folder>/env.py

# Add these lines
from alembic_utils.replaceable_entity import register_entities

from my_function import to_upper
from my_view import first_view

register_entities([to_upper, first_view])
```

You're done!

The next time you autogenerate a revision with
```shell
alembic revision --autogenerate -m 'some message'
```
Alembic will detect if your entities are new, updated, or removed & populate the revision's `upgrade` and `downgrade` sections automatically.

For example outputs, check the [examples](examples.md).
