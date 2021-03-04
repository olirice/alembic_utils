## Configuration


Controlling output of an `--autogenerate` revision is limited to including or excluding objects. By default, all schemas in the target database are included.

Selectively filtering `alembic_utils` objects can acheived using the `include_name` and `include_object` callables in `env.py`. For more information see [controlling-what-to-be-autogenerated](https://alembic.sqlalchemy.org/en/latest/autogenerate.html#controlling-what-to-be-autogenerated)


### Examples

The following examples perform common filtering tasks.

#### Whitelist of Schemas

Only include schemas from schemas named `public` and `bi`.

```python
# env.py

def include_name(name, type_, parent_names) -> bool:
    if type_ == "schema":
        return name in ["public", "bi"]
    return True

context.configure(
    # ...
    include_schemas = True,
    include_name = include_name
)
```

#### Exclude PGFunctions

Don't produce migrations for PGFunction entities.

```python
# env.py

def include_object(object, name, type_, reflected, compare_to) -> bool:
    if isinstance(object, PGFunction):
        return False
    return True

context.configure(
    # ...
    include_object=include_object
)
```
