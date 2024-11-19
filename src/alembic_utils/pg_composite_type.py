from typing import Generator

from parse import parse
from sqlalchemy import text as sql_text, TextClause

from alembic_utils.exceptions import SQLParseFailure
from alembic_utils.replaceable_entity import ReplaceableEntity
from alembic_utils.statement import strip_terminating_semicolon
from sqlalchemy.orm import Session


class PGCompositeType(ReplaceableEntity):
    """A PostgreSQL Composite Type compatible with `alembic revision --autogenerate`

    **Parameters:**

    * **schema** - *str*: A SQL schema name
    * **signature** - *str*: A SQL type's name
    * **definition** - *str*:  The type's definition including parenthesis (e.g. `(...)`)
    """

    type_ = "type"

    def __init__(self, schema: str, signature: str, definition: str):
        super().__init__(schema, signature, definition)
        self.definition: str = strip_terminating_semicolon(definition)

    @classmethod
    def from_sql(cls, sql: str) -> "PGCompositeType":
        """Create an instance from a SQL string"""
        template = "create{}type{:s}{schema}.{signature}{:s}as{:s}{definition}"
        result = parse(template, sql, case_sensitive=False)
        if result is not None:
            return cls(
                schema=result["schema"],
                # strip quote characters
                signature=result["signature"],
                definition=result["definition"],
            )

        raise SQLParseFailure(f'Failed to parse SQL into PGCompositeType """{sql}"""')

    def to_sql_statement_create(self) -> TextClause:
        """Generates a SQL "create type" statement"""
        return sql_text(
            f'CREATE TYPE {self.literal_schema}."{self.signature}" AS {self.definition};'
        )

    def to_sql_statement_drop(self, cascade=False) -> TextClause:
        """Generates a SQL "drop type" statement"""
        cascade = "cascade" if cascade else ""
        return sql_text(f'DROP TYPE {self.literal_schema}."{self.signature}" {cascade}')

    def to_sql_statement_create_or_replace(self) -> Generator[TextClause, None, None]:
        """Generates a SQL "create or replace type" statement"""
        attributes = [
            attr.split() for attr in self.definition.split("(", 1)[1].rsplit(")")[0].split(",")
        ]
        new_names = ", ".join([f"'{attr[0]}'" for attr in attributes])
        new_types = ", ".join([f"'{attr[1]}'" for attr in attributes])

        # if the type already exists, we need to alter it because types don't support "or replace"
        yield sql_text(f"""
            DO $$
            DECLARE
                v_type_name VARCHAR;
                v_schema VARCHAR;
                v_new_names VARCHAR[];
                v_new_types VARCHAR[];
                v_old_attrs pg_attribute[];
                v_position INTEGER;
            BEGIN
                CREATE TYPE {self.literal_schema}.{self.signature} AS {self.definition};
            EXCEPTION WHEN duplicate_object THEN
                v_schema := '{self.schema}';
                v_type_name := '{self.signature}';
                v_new_names := ARRAY[{new_names}]::VARCHAR[];
                v_new_types := ARRAY[{new_types}]::VARCHAR[];
            
                -- get old attributes
                SELECT
                    array_agg(a)
                INTO v_old_attrs
                FROM pg_type
                LEFT JOIN pg_namespace ON pg_type.typnamespace = pg_namespace.oid
                LEFT JOIN pg_class ON pg_type.typrelid = pg_class.oid
                LEFT JOIN pg_attribute a ON a.attrelid = pg_type.typrelid
                WHERE
                    typtype = 'c'
                    AND (pg_class.relkind IS NULL OR pg_class.relkind <> 'r')
                    AND nspname NOT IN ('pg_catalog', 'information_schema')
                    AND pg_type.typname = v_type_name
                    AND nspname::text like v_schema
                    AND a.attisdropped = false;
            
                IF v_old_attrs IS NOT NULL THEN
                    FOR i IN 1..cardinality(v_old_attrs) LOOP
                        v_position := array_position(v_new_names, v_old_attrs[i].attname);
                        IF v_position IS NOT NULL THEN
                            IF v_old_attrs[i].atttypid != to_regtype(v_new_types[v_position])
                            THEN
                                -- type of attribute changed
                                RAISE NOTICE 'Type % is not %', format_type(v_old_attrs[i].atttypid, v_old_attrs[i].atttypmod), v_new_types[v_position];
                                EXECUTE format('ALTER TYPE "%s".%s ALTER ATTRIBUTE %s SET DATA TYPE %s', v_schema, v_type_name, v_old_attrs[i].attname, v_new_types[v_position]);
                            END IF;
                        ELSE
                            -- attribute removed
                            RAISE NOTICE 'ALTER TYPE %.% DROP ATTRIBUTE %', v_schema, v_type_name, v_old_attrs[i].attname;
                            EXECUTE format('ALTER TYPE "%s".%s DROP ATTRIBUTE %s', v_schema, v_type_name, v_old_attrs[i].attname);
                        END IF;
                    END LOOP;
                END IF;
                FOR i IN 1..cardinality(v_new_names) LOOP
                    v_position := array_position(ARRAY(SELECT attname FROM unnest(v_old_attrs)), v_new_names[i]);
                    IF v_position IS NULL THEN
                        -- attribute added
                        RAISE NOTICE 'ALTER TYPE %.% ADD ATTRIBUTE % %', v_schema, v_type_name, v_new_names[i], v_new_types[i];
                        EXECUTE format('ALTER TYPE "%s".%s ADD ATTRIBUTE %s %s', v_schema, v_type_name, v_new_names[i], v_new_types[i]);
                    END IF;
                END LOOP;
            END;
            $$;
        """)

    @classmethod
    def from_database(cls, sess: Session, schema: str = "%"):
        """Get a list of all types defined in the db"""
        sql = sql_text(f"""
            SELECT
                nspname AS schema,
                pg_type.typname AS signature,
                string_agg(a.attname || ' ' || format_type(a.atttypid, a.atttypmod), ', ' ORDER BY a.attnum)
                    AS definition
            FROM pg_type
            LEFT JOIN pg_namespace ON pg_type.typnamespace = pg_namespace.oid
            LEFT JOIN pg_class ON pg_type.typrelid = pg_class.oid
            LEFT JOIN pg_attribute a ON a.attrelid = pg_type.typrelid
            WHERE
                typtype = 'c'
                AND (pg_class.relkind IS NULL OR pg_class.relkind <> 'r')
                AND nspname NOT IN ('pg_catalog', 'information_schema')
                AND nspname::text like '{schema}'
                AND a.attisdropped = false
            GROUP BY pg_type.typname, pg_namespace.nspname, pg_type.oid;
        """)
        rows = sess.execute(sql).fetchall()
        db_types = [cls(x[0], x[1], f"({x[2]})") for x in rows]

        for t in db_types:
            assert t is not None

        return db_types
