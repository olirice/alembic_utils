# pylint: disable=unused-argument,invalid-name,line-too-long
from typing import List, Tuple

from parse import parse
from sqlalchemy import text as sql_text

from alembic_utils.exceptions import FailedToGenerateComparable, SQLParseFailure
from alembic_utils.replaceable_entity import ReplaceableEntity


class PGTrigger(ReplaceableEntity):
    """A PostgreSQL Trigger compatible with `alembic revision --autogenerate`

    **Parameters:**

    * **schema** - *str*: A SQL schema name
    * **signature** - *str*: A SQL function's call signature
    * **definition** - *str*:  The remainig function body and identifiers

    Postgres Create Trigger Specification:

        CREATE [ CONSTRAINT ] TRIGGER name { BEFORE | AFTER | INSTEAD OF } { event [ OR ... ] }
        ON table
        [ FROM referenced_table_name ]
        [ NOT DEFERRABLE | [ DEFERRABLE ] { INITIALLY IMMEDIATE | INITIALLY DEFERRED } ]
        [ FOR [ EACH ] { ROW | STATEMENT } ]
        [ WHEN ( condition ) ]
        EXECUTE PROCEDURE function_name ( arguments )

    Limitations:
        - "table" must be qualified with a schema name e.g. public.account vs account
        - trigger schema must match table schema
    """

    _template = "create{:s}trigger{:s}{signature}{:s}{event}{:s}ON{:s}{on_entity}{:s}{action}"

    @classmethod
    def from_sql(cls, sql: str) -> "PGTrigger":
        """Create an instance instance from a SQL string"""
        result = parse(cls._template, sql, case_sensitive=False)
        if result is not None:
            # remove possible quotes from signature
            signature = result["signature"]
            event = result["event"]
            on_entity = result["on_entity"]
            action = result["action"]

            if "." not in on_entity:
                raise SQLParseFailure(
                    f'Failed to parse SQL into PGFunction the table/view {on_entity} must be qualified with a schema e.g. "public.account"'
                )

            schema = on_entity.split(".")[0]

            definition_template = " {event} ON {on_entity} {action}"
            definition = definition_template.format(event=event, on_entity=on_entity, action=action)

            return cls(
                schema=schema,
                signature=signature,
                definition=definition,
            )
        raise SQLParseFailure(f'Failed to parse SQL into PGTrigger """{sql}"""')

    def to_sql_statement_create(self) -> str:
        """ Generates a SQL "create function" statement for PGFunction """

        # We need to parse and replace the schema qualifier on the table for simulate_entity to
        # operate
        _def = self.definition
        _template = "{event}{:s}ON{:s}{on_entity}{:s}{action}"
        match = parse(_template, _def)
        if not match:
            raise SQLParseFailure(f'Failed to parse SQL into PGTrigger.definition """{_def}"""')

        event = match["event"]
        action = match["action"]

        # Ensure entity is qualified with schema
        on_entity = match["on_entity"]
        if "." in on_entity:
            _, _, on_entity = on_entity.partition(".")
        on_entity = f"{self.schema}.{on_entity}"

        # Re-render the definition ensuring the table is qualified with
        def_rendered = _template.replace("{:s}", " ").format(
            event=event, on_entity=on_entity, action=action
        )

        return sql_text(f"CREATE TRIGGER {self.signature} {def_rendered}")

    @property
    def on_entity(self) -> str:
        """Get the fully qualified name of the table/view the trigger is applied to"""
        create_statement = str(self.to_sql_statement_create())
        result = parse(self._template, create_statement, case_sensitive=False)
        return result["on_entity"]

    def to_sql_statement_drop(self) -> str:
        """Generates a SQL "drop function" statement for PGFunction"""
        return sql_text(f"DROP TRIGGER {self.signature} ON {self.on_entity};")

    def to_sql_statement_create_or_replace(self) -> str:
        """ Generates a SQL "create or replace function" statement for PGFunction """
        return f"""
        {self.to_sql_statement_drop()}
        {self.to_sql_statement_create()}
        """

    def get_definition_comparable(self, connection) -> Tuple:
        """Generates a SQL "create function" statement for PGTrigger

        Had to override create_entity because triggers inherit their schema from
        the table they're applied to
        """
        pg_version_str = connection.execute(sql_text("show server_version_num")).fetchone()[0]
        pg_version = int(pg_version_str)

        # First try a plain create
        definition_query = self.get_compare_definition_query()

        try:
            connection.execute("begin")
            connection.execute(self.to_sql_statement_create())

            return (self.schema,) + tuple(connection.execute(definition_query).fetchone())
        except Exception as exc:
            pass
        finally:
            connection.execute("rollback")

        # If  that fails, try a drop and then a create
        try:
            connection.execute("begin")
            connection.execute(self.to_sql_statement_drop())
            connection.execute(self.to_sql_statement_create())
            return (self.schema,) + tuple(connection.execute(definition_query).fetchone())
        except Exception as exc:
            pass
        finally:
            connection.execute("rollback")

        raise FailedToGenerateComparable("Could not simulate entity to get definition comparable")

    def get_identity_comparable(self, connection) -> Tuple:
        """Generates a SQL "create function" statement for PGTrigger

        Had to override create_entity because triggers inherit their schema from
        the table they're applied to
        """
        return (self.schema, self.identity)

    @classmethod
    def from_database(cls, connection, schema) -> List["PGFunction"]:
        """Get a list of all functions defined in the db"""

        # NOTE(OR): Schema is looked up by unqualified trigger name. Mismatches possible
        sql = sql_text(
            f"""
        select
            tgname trigger_name,
            pg_get_triggerdef(pgt.oid) definition,
            itr.trigger_schema as table_schema
        from
            pg_trigger pgt
            inner join information_schema.triggers itr
                    on lower(pgt.tgname) = lower(itr.trigger_name)
        where
            not tgisinternal
            and itr.event_object_schema = :schema
        """
        )

        rows = connection.execute(sql, schema=schema).fetchall()
        print(rows)

        db_triggers = [PGTrigger.from_sql(x[1]) for x in rows]

        for trig in db_triggers:
            assert trig is not None

        return db_triggers

    def get_compare_definition_query(self):
        return f"""
        select
            pg_get_triggerdef(pgt.oid) definition
        from
            pg_trigger pgt
            inner join information_schema.triggers itr
                on lower(pgt.tgname) = lower(itr.trigger_name)
        where
            not tgisinternal
            and itr.event_object_schema = '{self.schema}'
            and tgname = '{self.signature}'
        """
