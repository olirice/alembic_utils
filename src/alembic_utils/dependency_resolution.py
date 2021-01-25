import logging

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
