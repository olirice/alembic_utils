## Quickstart

### Installation
*Requirements* Python 3.7+

First, install alembic_utils
```shell
$ pip install alembic_utils
```

### Reference

Then add a function to your project
```python
# my_function.py
from alembic_utils.pg_function import PGFunction

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
```

Finally, update your `<migrations_folder>/env.py` to import the function and register it with alembic_utils.

```python
# <migrations_folder>/env.py

# Add these lines
from alembic_utils.replaceable_entity import register_entities
from my_function import to_upper

register_entities([to_upper])
```

You're done!

The next time you autogenerate a revision with
```shell
alembic revision --autogenerate -m 'some message'
```
Alembic will detect if your function is new, changed, or removed & populate the revison's `upgrade` and `downgrade` functions as appropriate.

For example outputs, check the [examples](examples.md).
