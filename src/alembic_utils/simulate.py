# pylint: disable=unused-argument,invalid-name,line-too-long
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
    deps: List["ReplaceableEntity"] = dependencies or []

    try:
        sess.begin_nested()

        dependency_managers = [simulate_entity(sess, x) for x in deps]

        with ExitStack() as stack:
            # Setup all the possible deps
            for mgr in dependency_managers:
                stack.enter_context(mgr)

            did_yield = False
            try:
                sess.begin_nested()
                sess.execute(entity.to_sql_statement_drop(cascade=True))
                sess.execute(entity.to_sql_statement_create())
                did_yield = True
                yield sess
            except:
                if did_yield:
                    # the error came from user code after the yield
                    # so we can exit
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
