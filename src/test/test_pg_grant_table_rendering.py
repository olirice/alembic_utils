"""Migration replay must preserve identifiers and historical recipients."""
from typing import Any, Dict

import pytest
from sqlalchemy import text

from alembic_utils.pg_grant_table import PGGrantTable


def replay(entity):
    namespace: Dict[str, Any] = {"PGGrantTable": PGGrantTable}
    exec(entity.render_self_for_migration(), namespace)
    return namespace[entity.to_variable_name()]


def test_legacy_public_migration_keeps_named_recipient():
    # Frozen output of the upstream renderer, before PUBLIC pseudo-role support.
    namespace: Dict[str, Any] = {"PGGrantTable": PGGrantTable}
    exec(
        "public_account_public_delete = PGGrantTable(schema='public', table='account', "
        "columns=[], role='PUBLIC', grant='DELETE', with_grant_option=False)",
        namespace,
    )
    entity = namespace["public_account_public_delete"]
    assert str(entity.to_sql_statement_create()) == 'GRANT DELETE ON "public"."account" TO "PUBLIC"'
    assert (
        str(entity.to_sql_statement_drop()) == 'REVOKE DELETE ON "public"."account" FROM "PUBLIC"'
    )


@pytest.mark.parametrize("field", ["schema", "table", "role", "columns"])
@pytest.mark.parametrize(
    "name",
    [
        "plain",
        "Mixed",
        "select",
        "two words",
        "a.b",
        'a"b',
        '"a"',
        '""a""',
        ":bind",
        "1start",
        "é",
        "a²",
    ],
)
def test_identifier_survives_migration_reconstruction(field, name):
    arguments: Dict[str, Any] = dict(
        schema="public", table="account", role="reader", grant="SELECT", columns=["a"]
    )
    # Supply a SQL-delimited identifier, including literal quote characters.
    quoted = '"' + name.replace('"', '""') + '"'
    arguments[field] = [quoted] if field == "columns" else quoted
    reconstructed = replay(PGGrantTable(**arguments))
    assert getattr(reconstructed, field) == ([name] if field == "columns" else name)


@pytest.mark.parametrize("role", [None, "PUBLIC", '"PUBLIC"', '"""PUBLIC"""', "None"])
def test_public_recipient_replay_and_reflection(sess, role):
    sess.execute(text('CREATE ROLE "PUBLIC"'))
    sess.execute(text('CREATE ROLE """PUBLIC"""'))
    sess.execute(text('CREATE ROLE "None"'))
    sess.execute(text("CREATE TABLE public.acl_replay (a int)"))
    entity = replay(PGGrantTable("public", "acl_replay", role, "DELETE"))
    sess.execute(entity.to_sql_statement_create())
    recipient = sess.execute(
        text(
            "SELECT a.grantee, r.rolname FROM pg_class c "
            "CROSS JOIN LATERAL aclexplode(c.relacl) a "
            "LEFT JOIN pg_roles r ON r.oid=a.grantee "
            "WHERE c.oid='acl_replay'::regclass AND a.grantee<>c.relowner"
        )
    ).one()
    expected = {
        None: None,
        "PUBLIC": "PUBLIC",
        '"PUBLIC"': "PUBLIC",
        '"""PUBLIC"""': '"PUBLIC"',
        "None": "None",
    }[role]
    assert recipient.rolname == expected
    assert (recipient.grantee == 0) == (role is None)
    reflected = [g for g in PGGrantTable.from_database(sess, "public") if g.table == "acl_replay"]
    assert len(reflected) == 1
    assert reflected[0].role == expected
    assert replay(reflected[0]).role == expected
    assert (
        PGGrantTable("public", "acl_replay", None, "DELETE").identity
        != PGGrantTable("public", "acl_replay", "None", "DELETE").identity
    )
    sess.execute(replay(reflected[0]).to_sql_statement_drop())
    assert not [g for g in PGGrantTable.from_database(sess, "public") if g.table == "acl_replay"]


def test_rendered_column_grant_targets_literal_quotes(sess):
    sess.execute(text("CREATE ROLE acl_quote_reader"))
    sess.execute(text('CREATE TABLE public.acl_quote_table (a int, """a""" int)'))
    entity = PGGrantTable("public", "acl_quote_table", "acl_quote_reader", "SELECT", ['"""a"""'])
    sess.execute(replay(entity).to_sql_statement_create())
    assert sess.execute(
        text("SELECT has_column_privilege('acl_quote_reader', 'acl_quote_table', :c, 'SELECT')"),
        {"c": '"a"'},
    ).scalar()
    assert not sess.execute(
        text("SELECT has_column_privilege('acl_quote_reader', 'acl_quote_table', 'a', 'SELECT')")
    ).scalar()
    reflected = [
        g for g in PGGrantTable.from_database(sess, "public") if g.table == "acl_quote_table"
    ]
    assert len(reflected) == 1
    assert replay(reflected[0]).columns == ['"a"']
