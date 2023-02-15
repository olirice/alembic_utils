from logging.config import fileConfig

from alembic import context
from sqlalchemy import MetaData, engine_from_config, pool

from alembic_utils.replaceable_entity import ReplaceableEntity

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = MetaData()

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def include_object(object, name, type_, reflected, compare_to) -> bool:
    # Do not generate migrations for non-alembic_utils entities
    if isinstance(object, ReplaceableEntity):
        # In order to test the application if this filter within
        # the autogeneration logic, apply a simple filter that
        # unit tests can relate to.
        #
        # In a 'real' implementation, this could be for example
        # ignoring entities from particular schemas.
        return not "exclude_obj_" in name
    else:
        return False


def include_name(name, type_, parent_names) -> bool:
    # In order to test the application if this filter within
    # the autogeneration logic, apply a simple filter that
    # unit tests can relate to
    return not "exclude_name_" in name if name else True


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_schemas=True,
            include_object=include_object,
            include_name=include_name,
        )

        with context.begin_transaction():
            context.run_migrations()


run_migrations_online()
