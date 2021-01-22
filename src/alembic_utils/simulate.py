# pylint: disable=unused-argument,invalid-name,line-too-long
import logging
from contextlib import ExitStack, contextmanager
from typing import List, Optional, TypeVar

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="ReplaceableEntity")


@contextmanager
def simulate_entity(sess: Session, entity, dependencies: Optional[List[T]] = None):
    """Creates *entiity* in a transaction so postgres rendered definition
    can be retrieved
    """
    if not dependencies:
        dependencies: List[T] = []

    try:
        sess.begin_nested()

        dependency_managers = [simulate_entity(sess, x) for x in dependencies]

        with ExitStack() as stack:
            # Setup all the possible dependencies
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
