from contextlib import contextmanager
from functools import partial
from typing import TYPE_CHECKING, Dict, List, Optional

from parse import parse
from sqlalchemy.orm import Session

from alembic_utils.statement import strip_double_quotes

if TYPE_CHECKING:
    from alembic_utils.replaceable_entity import ReplaceableEntity


def get_dependent_entities(sess: Session, entity: "ReplaceableEntity") -> List["ReplaceableEntity"]:
    """Collect a list of entities that are dependent on *entity*

    The output is sorted in the order that the entities can be dropped without raising
    an exception. It includes direct and transitive dependencies
    """
    from alembic_utils.pg_materialized_view import PGMaterializedView
    from alembic_utils.pg_trigger import PGTrigger
    from alembic_utils.pg_view import PGView

    # Materialized views don't have a create or replace statement
    # So replace_entity is defined as a drop and then a create
    # the simulator will fail to simulate the drop and complain about dependent entities
    # if they exist
    # The dependency resolver defers dependent entities while we figure out what
    # migration op is needed and then rolls them all back in a transaction

    sql_error_message: Optional[str] = None
    try:
        sess.begin_nested()
        sess.execute(entity.to_sql_statement_drop())
    except Exception as exc:
        sql_error_message = str(exc)
    finally:
        sess.rollback()

    if sql_error_message is None:
        # No dependencies
        return []

    # Parse the error to find the dependency links
    #
    # (psycopg2.errors.DependentObjectsStillExist) cannot drop view abc.account because other objects depend on it
    # DETAIL:  view xyz.contact depends on view abc.account
    # view abc.contact depends on view xyz.contact
    # materialized view xyz.goog depends on view xyz.contact
    # HINT: USE DROP ... CASCADE to drop the dependent objects too.
    # .....

    error_template = "{}cannot drop{}{}DETAIL:{remaining}HINT:{}{:w}{}"

    view_template = "view {signature} depends on {}"
    materialized_view_template = "materialized view {signature} depends on {}"
    trigger_template = "trigger {signature} on table {on_entity} depends on {}"

    # Dependencies of functions are not tracked or enforced in PG. Their bodies are stored as text
    #
    # Example:
    #    create view public.abc as select 'hello' as one;
    #    create or replace function catabc() returns text as $$ select one from public.abc $$ language sql;
    #    select catabc()
    #    -- Returns: 'hello'
    #
    #    drop view public.abc
    #    -- Ideally this would fail, but it doesnt.
    #
    #    select catabc()
    #    -- Runtime error:
    #    -- relation "public.abc" does not exist

    res = parse(error_template, sql_error_message)

    if not res:
        # The error was something other than a dependency issue
        return []

    class_to_parser = [
        (PGView, partial(parse, view_template)),
        (PGMaterializedView, partial(parse, materialized_view_template)),
        (PGTrigger, partial(parse, trigger_template)),
    ]

    dependent_objects = []
    if res:
        dependency_records = res["remaining"].strip().split("\n")

        for drec in dependency_records:

            for class_, parser in class_to_parser:

                parsed_signature = parser(drec)

                if parsed_signature:

                    signature_str = parsed_signature["signature"]
                    if "." in signature_str:
                        schema, _, signature = signature_str.partition(".")
                    else:
                        schema, signature = "public", signature_str

                    init_dict: Dict[str, str] = parsed_signature.named
                    init_dict["schema"] = strip_double_quotes(schema)
                    init_dict["signature"] = strip_double_quotes(signature)
                    init_dict["definition"] = ""

                    entity_wo_definition = class_(**init_dict)
                    dependent_objects.append(entity_wo_definition)
                    break

    seen_identities = set()
    deduped_dependencies = []

    for ent in reversed(dependent_objects):
        if ent.identity not in seen_identities:
            deduped_dependencies.append(ent)
        seen_identities.add(ent.identity)

    # This includes transitive dependencies
    return deduped_dependencies


@contextmanager
def defer_dependent(sess: Session, entity: "ReplaceableEntity"):
    """Defer entities that depend on *entity* and yield the session

    Automatically rolls back at the end of the context
    """

    dependents = get_dependent_entities(sess, entity)

    try:
        sess.begin_nested()

        for ent in dependents:
            sess.execute(ent.to_sql_statement_drop())
        yield dependents

    finally:
        sess.rollback()
