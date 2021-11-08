# pylint: disable=unused-argument,invalid-name,line-too-long
import copy
import logging
from contextlib import ExitStack, contextmanager
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy.orm import Session

if TYPE_CHECKING:
    from alembic_utils.replaceable_entity import ReplaceableEntity


logger = logging.getLogger(__name__)


@contextmanager
def simulate_entity(
    sess: Session,
    entity: "ReplaceableEntity",
    dependencies: Optional[List["ReplaceableEntity"]] = None,
):
    """Creates *entiity* in a transaction so postgres rendered definition
    can be retrieved
    """

    # When simulating materialized view, don't populate them with data
    from alembic_utils.pg_materialized_view import PGMaterializedView

    if isinstance(entity, PGMaterializedView) and entity.with_data:
        entity = copy.deepcopy(entity)
        entity.with_data = False

    deps: List["ReplaceableEntity"] = dependencies or []

    try:
        sess.begin_nested()

        dependency_managers = [simulate_entity(sess, x) for x in deps]

        with ExitStack() as stack:
            # Setup all the possible deps
            for mgr in dependency_managers:
                stack.enter_context(mgr)

            did_drop = False
            try:
                sess.begin_nested()
                sess.execute(entity.to_sql_statement_drop(cascade=True))
                did_drop = True
                sess.execute(entity.to_sql_statement_create())
                yield sess
            except:
                if did_drop:
                    # The drop was successful, so either create was not, or the
                    # error came from user code after the yield.
                    # Anyway, we can exit now.
                    raise

                # Try again without the drop in case the drop raised
                # a does not exist error
                sess.rollback()
                sess.begin_nested()
                sess.execute(entity.to_sql_statement_create())
                yield sess
            finally:
                sess.rollback()
    finally:
        sess.rollback()
