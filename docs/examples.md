## Example Outputs

### Migration for newly created function
```python
"""create

Revision ID: 1
Revises:
Create Date: 2020-04-22 09:24:25.556995
"""
from alembic import op
import sqlalchemy as sa
from alembic_utils import PGFunction

# revision identifiers, used by Alembic.
revision = '1'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    public_to_upper_6fa0de = PGFunction(
            schema="public",
            signature="to_upper(some_text text)",
            definition="""
            returns text
            as
            $$ select upper(some_text) $$ language SQL;
            """
        )

    op.create_function(public_to_upper_6fa0de)


def downgrade():
    public_to_upper_6fa0de = PGFunction(
            schema="public",
            signature="to_upper(some_text text)",
            definition="# Not Used"
        )

    op.drop_function(public_to_upper_6fa0de)
```

### Migration for updated Function

```python
"""replace

Revision ID: 2
Revises: 1
Create Date: 2020-04-22 09:24:25.679031
"""
from alembic import op
import sqlalchemy as sa
from alembic_utils import PGFunction

# revision identifiers, used by Alembic.
revision = '2'
down_revision = '1'
branch_labels = None
depends_on = None


def upgrade():
    public_to_upper_6fa0de = PGFunction(
            schema="public",
            signature="to_upper(some_text text)",
            definition="""
            returns text
            as
            $$ select upper(some_text) || 'def' $$ language SQL;
        """
        )
    op.replace_function(public_to_upper_6fa0de)


def downgrade():
    public_to_upper_6fa0de = PGFunction(
            schema="public",
            signature="to_upper(some_text text)",
            definition="""returns text
     LANGUAGE sql
    AS $function$ select upper(some_text) || 'abc' $function$"""
        )
    op.replace_function(public_to_upper_6fa0de)
```