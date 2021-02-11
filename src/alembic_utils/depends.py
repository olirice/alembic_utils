import logging
from contextlib import contextmanager
from typing import Generator, List

from sqlalchemy import exc as sqla_exc
from sqlalchemy.orm import Session

from alembic_utils.simulate import simulate_entity

logger = logging.getLogger(__name__)


def solve_resolution_order(sess: Session, entities):
    """Solve for an entity resolution order that increases the probability that
    a migration will suceed if, for example, two new views are created and one
    refers to the other

    This strategy will only solve for simple cases
    """

    resolved = []

    # Resolve the entities with 0 dependencies first (faster)
    logger.info("Resolving entities with no dependencies")
    for entity in entities:
        try:
            with simulate_entity(sess, entity):
                resolved.append(entity)
        except (sqla_exc.ProgrammingError, sqla_exc.InternalError) as exc:
            continue

    # Resolve entities with possible dependencies
    for _ in range(len(entities)):
        logger.info("Resolving entities with dependencies. This may take a minute")
        n_resolved = len(resolved)

        for entity in entities:
            if entity in resolved:
                continue

            try:
                with simulate_entity(sess, entity, dependencies=resolved):
                    resolved.append(entity)
            except (sqla_exc.ProgrammingError, sqla_exc.InternalError):
                continue

        if len(resolved) == n_resolved:
            # No new entities resolved in the last iteration. Exit
            break

    for entity in entities:
        if entity not in resolved:
            resolved.append(entity)

    return resolved


@contextmanager
def recreate_dropped(connection) -> Generator[Session, None, None]:
    """Recreate any dropped all ReplaceableEntities that were dropped within block

    This is useful for making cascading updates. For example, updating a table's column type when it has dependent views.

    def upgrade() -> None:

        my_view = PGView(...)

        with recreate_dropped(op.get_bind()) as conn:

            op.drop_entity(my_view)

            # change an integer column to a bigint
            op.alter_column(
                table_name="account",
                column_name="id",
                schema="public"
                type_=sa.BIGINT()
                existing_type=sa.Integer(),
            )
    """
    from alembic_utils.pg_function import PGFunction
    from alembic_utils.pg_materialized_view import PGMaterializedView
    from alembic_utils.pg_trigger import PGTrigger
    from alembic_utils.pg_view import PGView
    from alembic_utils.replaceable_entity import ReplaceableEntity

    # Do not include permissions here e.g. PGGrantTable. If columns granted to users are dropped, it will cause an error

    def collect_all_db_entities(sess: Session) -> List[ReplaceableEntity]:
        """Collect all entities from the database"""

        return [
            *PGFunction.from_database(sess, "%"),
            *PGTrigger.from_database(sess, "%"),
            *PGView.from_database(sess, "%"),
            *PGMaterializedView.from_database(sess, "%"),
        ]

    sess = Session(bind=connection)

    # All existing entities, before the upgrade
    before = collect_all_db_entities(sess)

    # In the yield, do a
    #     op.drop_entity(my_mat_view, cascade=True)
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
