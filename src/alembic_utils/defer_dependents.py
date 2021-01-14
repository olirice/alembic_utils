from contextlib import contextmanager
from functools import partial
from typing import TYPE_CHECKING, Optional

from parse import parse
from sqlalchemy.orm import Session

from alembic_utils.statement import strip_double_quotes

if TYPE_CHECKING:
    from alembic_utils.replaceable_entity import ReplaceableEntity


@contextmanager
def defer_dependents(sess: Session, entity: "ReplaceableEntity"):
    """Defer entities that depend on *entity*"""

    # Materialized views don't have a create or replace statement
    # So replace_entity is defined as a drop and then a create
    # the simulator will fail to simulate the drop and complain about dependent entities
    # if they exist

    # The dependency resolver defers dependent entities while we figure out what
    # migration op is needed and then rolls them all back in a transaction

    from alembic_utils.pg_materialized_view import PGMaterializedView
    from alembic_utils.pg_view import PGView

    # This is almost certainly a bad idea

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
        yield
        return

    # Parse the error to find the dependency links
    #
    # (psycopg2.errors.DependentObjectsStillExist) cannot drop view abc.account because other objects depend on it
    # DETAIL:  view xyz.contact depends on view abc.account
    # view abc.contact depends on view xyz.contact
    # materialized view xyz.goog depends on view xyz.contact
    # HINT: USE DROP ... CASCADE to drop the dependent objects too.
    # .....

    TEMPLATE = "{}cannot drop{}{}DETAIL:{remaining}HINT:{}{:w}{}"

    VIEW_RECORD_TEMPLATE = "view {signature} depends on {}"
    MATERIALIZED_VIEW_RECORD_TEMPLATE = "materialized view {signature} depends on {}"

    res = parse(TEMPLATE, sql_error_message)

    if not res:
        # The error was something other than a dependency issue
        yield
        return

    class_to_parser = [
        (PGView, partial(parse, VIEW_RECORD_TEMPLATE)),
        (PGMaterializedView, partial(parse, MATERIALIZED_VIEW_RECORD_TEMPLATE)),
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

                    entity_wo_definition = class_(
                        strip_double_quotes(schema), strip_double_quotes(signature), ""
                    )

                    dependent_objects.append(entity_wo_definition)

                    # If definition is needed in the future, use the following

                    # dependent_object = [
                    #    x
                    #    for x in class_.from_database(sess, schema)
                    #    if x.identity == entity_wo_definition.identity
                    # ][0]

                    # dependent_objects.append(dependent_object)
                    break

    # This includes transitive dependencies
    dependent_objects = list(reversed(dependent_objects))

    try:
        sess.begin_nested()

        for ent in dependent_objects:
            sess.execute(ent.to_sql_statement_drop())
        yield

    finally:
        sess.rollback()
