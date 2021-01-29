from contextlib import contextmanager
from typing import List

from sqlalchemy.orm import Session

from alembic_utils.dependency_resolution import solve_resolution_order
from alembic_utils.pg_function import PGFunction
from alembic_utils.pg_materialized_view import PGMaterializedView
from alembic_utils.pg_trigger import PGTrigger
from alembic_utils.pg_view import PGView
from alembic_utils.replaceable_entity import ReplaceableEntity


def collect_all_db_entities(sess: Session) -> List[ReplaceableEntity]:
    """Collect all entities from the database"""

    return [
        *PGFunction.from_database(sess, "%"),
        *PGTrigger.from_database(sess, "%"),
        *PGView.from_database(sess, "%"),
        *PGMaterializedView.from_database(sess, "%"),
    ]


@contextmanager
def defer_during_upgrade(connection):
    """WARNING: UNSAFE!
    Drop and then rebuild dependencies on *entity* to allow more aggressive changes
    """
    sess = Session(bind=connection)

    # All existing entities, before the upgrade
    before = collect_all_db_entities(sess)

    # In the yield, do a
    #     conn.execute(my_mat_view.to_drop_entity(cascade=True))
    #     op.create_entity(my_mat_view)
    try:
        yield sess
    except:
        sess.rollback()
        raise

    # All existing entities, after the upgrade
    after = collect_all_db_entities(sess)
    after_identities = {x.identity for x in after}

    # Entities that were not impacted, or that we have "recovered"
    resolved = []
    unresolved = []

    # First, ignore the ones that were not impacted by the upgrade
    for ent in before:
        if ent.identity in after_identities:
            resolved.append(ent)
        else:
            unresolved.append(ent)

    # Attempt to find an acceptable order of creation for the unresolved entities
    ordered_unresolved = solve_resolution_order(sess, unresolved)

    # Attempt to recreate the missing entities in the specified order
    for ent in ordered_unresolved:
        sess.execute(ent.to_sql_statement_create())

    # Sanity check that everything is now fine
    sanity_check = collect_all_db_entities(sess)
    # Fail and rollback if the sanity check is wrong
    try:
        assert len(before) == len(sanity_check)
    except:
        sess.rollback()
        raise

    # Close out the session
    sess.commit()
