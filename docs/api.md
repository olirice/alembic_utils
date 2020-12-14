## API Reference

::: alembic_utils.replaceable_entity.register_entities
    :docstring:

```python
# migrations/env.py

from alembic_utils.replaceable_entity import register_entities
from app.functions import my_function
from app.views import my_view

register_entities(entities=[my_function, my_view], exclude_schema=['audit'])
```

::: alembic_utils.pg_function.PGFunction
    :docstring:
    :members: from_sql from_path

```python
from alembic_utils.pg_function import PGFunction

to_lower = PGFunction(
    schema="public",
    signature="to_lower(some_text text)",
    definition="returns text as $$ lower(some_text) $$ language sql"
)
```


::: alembic_utils.pg_view.PGView
    :docstring:
    :members: from_sql from_path


```python
from alembic_utils.pg_view import PGView

scifi_books = PGView(
    schema="public",
    signature="scifi_books",
    definition="select * from books where genre='scifi'"
)
```


::: alembic_utils.pg_trigger.PGTrigger
    :docstring:
    :members: from_sql from_path


```python
from alembic_utils.pg_trigger import PGTrigger

trigger = PGTrigger(
    schema="public",
    signature="lower_account_email",
    definition="""
        BEFORE INSERT ON public.account
        FOR EACH ROW EXECUTE FUNCTION public.downcase_email()
    """,
)
```
