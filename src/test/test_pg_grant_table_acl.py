"""Explicit PostgreSQL ACLs, independent of information_schema's expansion."""
import pytest
from sqlalchemy import text

from alembic_utils.exceptions import BadInputException
from alembic_utils.pg_grant_table import PGGrantTable
from alembic_utils.replaceable_entity import register_entities
from alembic_utils.testbase import TEST_VERSIONS_ROOT, run_alembic_command


@pytest.fixture
def acl_setup(sess):
    sess.execute(text("CREATE ROLE acl_reader NOLOGIN"))
    sess.execute(text("CREATE ROLE acl_delegate NOLOGIN"))
    sess.execute(text("CREATE TABLE public.acl_table (a int, b int)"))
    sess.execute(text("GRANT USAGE ON SCHEMA public TO acl_delegate"))
    return sess


def reflect(sess, table="acl_table", role="acl_reader"):
    return [
        (g.grant.value, g.columns, g.with_grant_option)
        for g in PGGrantTable.from_database(sess, "public")
        if g.table == table and g.role == role
    ]


@pytest.mark.parametrize(
    "privilege", ["SELECT", "INSERT", "UPDATE", "REFERENCES", "DELETE", "TRUNCATE", "TRIGGER"]
)
@pytest.mark.parametrize("grant_option", [False, True])
def test_table_grants_round_trip(acl_setup, privilege, grant_option):
    sess = acl_setup
    entity = PGGrantTable(
        "public", "acl_table", "acl_reader", privilege, with_grant_option=grant_option
    )
    sess.execute(entity.to_sql_statement_create())
    assert reflect(sess) == [(privilege, [], grant_option)]
    assert sess.execute(
        text("SELECT has_table_privilege('acl_reader', 'public.acl_table', :p)"), {"p": privilege}
    ).scalar()
    sess.execute(entity.to_sql_statement_drop())
    assert reflect(sess) == []


@pytest.mark.parametrize("privilege", ["SELECT", "INSERT", "UPDATE", "REFERENCES"])
@pytest.mark.parametrize("grant_option", [False, True])
def test_column_grants_round_trip(acl_setup, privilege, grant_option):
    sess = acl_setup
    entity = PGGrantTable("public", "acl_table", "acl_reader", privilege, ["b", "a"], grant_option)
    sess.execute(entity.to_sql_statement_create())
    assert reflect(sess) == [(privilege, ["a", "b"], grant_option)]
    assert not sess.execute(
        text("SELECT has_table_privilege('acl_reader', 'acl_table', :p)"), {"p": privilege}
    ).scalar()
    sess.execute(entity.to_sql_statement_drop())
    assert reflect(sess) == []


@pytest.mark.parametrize("columns", [None, []])
def test_table_grants_cover_future_columns(acl_setup, columns):
    sess = acl_setup
    sess.execute(
        PGGrantTable(
            "public", "acl_table", "acl_reader", "SELECT", columns
        ).to_sql_statement_create()
    )
    sess.execute(text("ALTER TABLE acl_table ADD COLUMN c int"))
    assert reflect(sess) == [("SELECT", [], False)]
    assert sess.execute(
        text("SELECT has_column_privilege('acl_reader', 'acl_table', 'c', 'SELECT')")
    ).scalar()


def test_table_and_column_acl_entries_are_not_merged(acl_setup):
    sess = acl_setup
    sess.execute(text("GRANT SELECT ON acl_table TO acl_reader"))
    sess.execute(text("GRANT SELECT (a) ON acl_table TO acl_reader"))
    assert sorted(reflect(sess)) == [("SELECT", [], False), ("SELECT", ["a"], False)]


def test_delegated_grants_are_visible_and_duplicate_grantors_coalesce(acl_setup):
    sess = acl_setup
    sess.execute(text("GRANT SELECT ON acl_table TO acl_delegate WITH GRANT OPTION"))
    sess.execute(text("SET LOCAL ROLE acl_delegate"))
    sess.execute(text("GRANT SELECT ON acl_table TO acl_reader"))
    sess.execute(text("RESET ROLE"))
    assert reflect(sess) == [("SELECT", [], False)]
    sess.execute(text("GRANT SELECT ON acl_table TO acl_reader WITH GRANT OPTION"))
    assert reflect(sess) == [("SELECT", [], True)]
    # A revoke as the owner cannot erase the delegated ACL entry.
    entity = PGGrantTable("public", "acl_table", "acl_reader", "SELECT")
    sess.execute(entity.to_sql_statement_drop())
    assert reflect(sess) == [("SELECT", [], False)]
    sess.execute(text("SET LOCAL ROLE acl_delegate"))
    sess.execute(entity.to_sql_statement_drop())
    sess.execute(text("RESET ROLE"))
    assert reflect(sess) == []


def test_delegated_column_grants_are_visible(acl_setup):
    sess = acl_setup
    sess.execute(text("GRANT UPDATE (a) ON acl_table TO acl_delegate WITH GRANT OPTION"))
    sess.execute(text("SET LOCAL ROLE acl_delegate"))
    sess.execute(text("GRANT UPDATE (a) ON acl_table TO acl_reader"))
    sess.execute(text("RESET ROLE"))
    assert reflect(sess) == [("UPDATE", ["a"], False)]


@pytest.mark.parametrize("columns", [None, ["a"]])
def test_public_grants_create_reflect_revoke(acl_setup, columns):
    sess = acl_setup
    entity = PGGrantTable("public", "acl_table", "PUBLIC", "SELECT", columns)
    assert "TO PUBLIC" in entity.to_sql_statement_create().text
    sess.execute(entity.to_sql_statement_create())
    assert reflect(sess, role="PUBLIC") == [("SELECT", columns or [], False)]
    sess.execute(entity.to_sql_statement_drop())
    assert reflect(sess, role="PUBLIC") == []


@pytest.mark.parametrize("column", ["Mixed", "select", "two words", "a.b", 'a"b', ":bind"])
def test_column_identifiers_are_quoted(acl_setup, column):
    sess = acl_setup
    escaped = column.replace('"', '""')
    # exec_driver_sql avoids SQLAlchemy text parameter parsing in the independent fixture.
    sess.connection().exec_driver_sql(f'ALTER TABLE acl_table ADD COLUMN "{escaped}" int')
    entity = PGGrantTable("public", "acl_table", "acl_reader", "SELECT", [column])
    sess.execute(entity.to_sql_statement_create())
    assert reflect(sess) == [("SELECT", [column], False)]
    assert sess.execute(
        text("SELECT has_column_privilege('acl_reader', 'acl_table', :c, 'SELECT')"), {"c": column}
    ).scalar()


def test_dropped_columns_are_not_reflected(acl_setup):
    sess = acl_setup
    sess.execute(text("GRANT SELECT (a, b) ON acl_table TO acl_reader"))
    sess.execute(text("ALTER TABLE acl_table DROP COLUMN a"))
    assert reflect(sess) == [("SELECT", ["b"], False)]


def test_sequence_grants_are_not_table_grants(acl_setup):
    sess = acl_setup
    sess.execute(text("CREATE SEQUENCE acl_sequence"))
    sess.execute(text("GRANT SELECT ON SEQUENCE acl_sequence TO acl_reader"))
    assert reflect(sess, table="acl_sequence") == []


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE VIEW acl_relation AS SELECT * FROM acl_table",
        "CREATE MATERIALIZED VIEW acl_relation AS SELECT * FROM acl_table",
        "CREATE TABLE acl_relation (a int) PARTITION BY RANGE (a)",
    ],
)
def test_other_table_like_relations(acl_setup, statement):
    sess = acl_setup
    sess.execute(text(statement))
    sess.execute(text("GRANT SELECT ON acl_relation TO acl_reader"))
    assert reflect(sess, table="acl_relation") == [("SELECT", [], False)]


def test_owner_and_superuser_acl_entries_are_excluded(acl_setup):
    sess = acl_setup
    sess.execute(text("ALTER TABLE acl_table OWNER TO acl_delegate"))
    sess.execute(text("GRANT SELECT ON acl_table TO acl_reader"))
    assert reflect(sess, role="acl_delegate") == []
    assert reflect(sess) == [("SELECT", [], False)]
    assert all(g.role != "alem_user" for g in PGGrantTable.from_database(sess, "public"))


@pytest.mark.parametrize("table_to_column", [True, False])
@pytest.mark.parametrize("role", ["anon_user", "PUBLIC"])
def test_autogenerated_scope_change_upgrade_downgrade(engine, table_to_column, role):
    before = [] if table_to_column else ["a", "b"]
    after = ["a", "b"] if table_to_column else []
    with engine.begin() as conn:
        conn.execute(text("CREATE ROLE anon_user"))
        conn.execute(text("CREATE TABLE acl_table (a int, b int)"))
        conn.execute(
            text(f'GRANT SELECT {"" if table_to_column else "(a, b)"} ON acl_table TO {role}')
        )
    target = PGGrantTable("public", "acl_table", role, "SELECT", after)
    register_entities([target], entity_types=[PGGrantTable])
    run_alembic_command(
        engine, "revision", {"autogenerate": True, "rev_id": "1", "message": "scope"}
    )
    migration = (TEST_VERSIONS_ROOT / "1_scope.py").read_text()
    assert "op.replace_entity" in migration
    assert "op.create_entity" not in migration
    assert "op.drop_entity" not in migration

    def assert_scope(expected):
        with engine.connect() as conn:
            table = conn.execute(
                text("SELECT has_table_privilege(:role, 'acl_table', 'SELECT')"),
                {"role": role.lower() if role == "PUBLIC" else role},
            ).scalar()
            assert table == (not expected)
            columns = (
                conn.execute(
                    text(
                        "SELECT attname FROM pg_attribute WHERE attrelid='acl_table'::regclass AND attacl IS NOT NULL ORDER BY attname"
                    )
                )
                .scalars()
                .all()
            )
            assert columns == expected

    run_alembic_command(engine, "upgrade", {"revision": "head"})
    assert_scope(after)
    run_alembic_command(engine, "downgrade", {"revision": "base"})
    assert_scope(before)
    run_alembic_command(engine, "upgrade", {"revision": "head"})
    assert_scope(after)
    run_alembic_command(
        engine, "revision", {"autogenerate": True, "rev_id": "2", "message": "noop"}
    )
    noop = (TEST_VERSIONS_ROOT / "2_noop.py").read_text()
    assert "op.replace_entity" not in noop
    assert "op.create_entity" not in noop
    assert "op.drop_entity" not in noop


def test_quoted_public_role_is_not_the_public_pseudo_role(acl_setup):
    sess = acl_setup
    sess.execute(text('CREATE ROLE "PUBLIC"'))
    entity = PGGrantTable("public", "acl_table", '"PUBLIC"', "SELECT")
    sess.execute(entity.to_sql_statement_create())
    assert reflect(sess, role='"PUBLIC"') == [("SELECT", [], False)]
    assert reflect(sess, role="PUBLIC") == []
    # Independent ACL OID check: granting a real role must not grant everybody.
    assert (
        sess.execute(
            text(
                "SELECT count(*) FROM pg_class c, LATERAL aclexplode(c.relacl) a WHERE c.oid='acl_table'::regclass AND a.grantee=0"
            )
        ).scalar()
        == 0
    )
    sess.execute(entity.to_sql_statement_drop())
    assert reflect(sess, role='"PUBLIC"') == []


def test_prequoted_column_is_normalized(acl_setup):
    sess = acl_setup
    sess.execute(text('ALTER TABLE acl_table ADD COLUMN "Mixed" int'))
    entity = PGGrantTable("public", "acl_table", "acl_reader", "SELECT", ['"Mixed"'])
    sess.execute(entity.to_sql_statement_create())
    assert reflect(sess) == [("SELECT", ["Mixed"], False)]


@pytest.mark.parametrize(
    "second_grant",
    [
        "GRANT SELECT (a) ON acl_table TO acl_reader",
        "GRANT SELECT (a) ON acl_table TO acl_reader WITH GRANT OPTION",
    ],
)
def test_mixed_acl_groups_refuse_lossy_replacement(acl_setup, second_grant):
    sess = acl_setup
    sess.execute(text("GRANT SELECT ON acl_table TO acl_reader"))
    sess.execute(text(second_grant))
    before = reflect(sess)
    entity = PGGrantTable("public", "acl_table", "acl_reader", "SELECT", ["b"])
    with pytest.raises(BadInputException, match="Multiple ACL groups"):
        entity.get_required_migration_op(sess)
    assert reflect(sess) == before


@pytest.mark.parametrize("privilege", ["DELETE", "TRUNCATE", "TRIGGER"])
def test_table_only_privileges_reject_columns(privilege):
    with pytest.raises(BadInputException, match="must not be provided"):
        PGGrantTable("public", "acl_table", "acl_reader", privilege, ["a"])
