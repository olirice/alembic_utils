import pytest
from alembic.util import AutogenerateDiffsDetected

from alembic_utils.pg_view import PGView
from alembic_utils.replaceable_entity import register_entities
from alembic_utils.testbase import run_alembic_command

TEST_VIEW_before = PGView(
    schema="public",
    signature="testExample",
    definition="select feature_name from information_schema.sql_features",
)
TEST_VIEW_after = PGView(
    schema="public",
    signature="testExample",
    definition="select feature_name, is_supported from information_schema.sql_features",
)

def test_check_diff_create(engine) -> None:
    register_entities([TEST_VIEW_before])

    with pytest.raises(AutogenerateDiffsDetected) as e_info:
        run_alembic_command(engine, "check", {})

    exp = (
        "New upgrade operations detected: "
        "[('create_entity', 'PGView: public.testExample', "
        '\'CREATE VIEW "testExample" AS select feature_name from information_schema.sql_features;\')]'
    )
    assert e_info.value.args[0] == exp


def test_check_diff_upgrade(engine) -> None:
    with engine.begin() as connection:
        connection.execute(TEST_VIEW_before.to_sql_statement_create())

    register_entities([TEST_VIEW_after])

    with pytest.raises(AutogenerateDiffsDetected) as e_info:
        run_alembic_command(engine, "check", {})

    assert e_info.value.args[0].startswith(
        "New upgrade operations detected: [('replace_or_revert_entity', 'PGView: public.testExample'"
    )


def test_check_diff_drop(engine) -> None:
    with engine.begin() as connection:
        connection.execute(TEST_VIEW_before.to_sql_statement_create())

    register_entities([])

    with pytest.raises(AutogenerateDiffsDetected) as e_info:
        run_alembic_command(engine, "check", {})

    exp = "New upgrade operations detected: [('drop_entity', 'PGView: public.testExample')]"

    assert e_info.value.args[0] == exp
