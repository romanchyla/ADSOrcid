"""Adding key-value table

Revision ID: 456fd4e10658
Revises: 4475ef3e98af
Create Date: 2015-11-05 01:47:04.347535

"""

# revision identifiers, used by Alembic.
revision = '456fd4e10658'
down_revision = '4475ef3e98af'

from alembic import op
import sqlalchemy as sa
from sqlalchemy import Column, String, Text
                               


def upgrade():
    op.create_table('storage',
        Column('key', String(255), primary_key=True),
        Column('value', Text),
    )


def downgrade():
    op.drop_table('storage')
